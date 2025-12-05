package whu.edu.moniData.ingest.holyAnalysisJob.redisAndHbase.agoVersions;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import whu.edu.moniData.ingest.holyAnalysisJob.redisAndHbase.agoVersions.Utils.PathPoint;
import lombok.*;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.time.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import static whu.edu.moniData.ingest.holyAnalysisJob.redisAndHbase.agoVersions.Utils.convertToTimestampMillis;

/**
 * SegCarIngestSimulatedV1
 * 9.16
 *  1. 设计rowkey，时间为主维度
 *  2. 表每季度新建一次
 *  3. 更新存储结构，计算平均速度
 * 9.20
 *  1. 开启检查点
 *
 */
public class SegCarIngestSimulatedV1 {
    public static void main(String[] args) throws Exception {
        // 设置 Flink 流执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("hdfs://namenode:9000/flink/checkpoints/storage");
        env.setParallelism(4);

        // 配置 KafkaSource
        String brokers = args[0];
        String groupId = "flink-group-SegCar"; // 消费者组ID

        // 主题列表
        List<String> topics = Arrays.asList(args[1].split(","));

        // 初始化第一个 KafkaSource
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(topics)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setProperty("auto.offset.commit", "true")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 创建第一个数据流
        DataStream<String> unionStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Sources Save");

        // 保存 flatMap 操作后的结果
        DataStream<PathPoint> flatMapStream = unionStream
                .flatMap(new FlatMapFunction<String, PathPoint>() {
                    @Override
                    public void flatMap(String jsonString, Collector<PathPoint> out) {
                        try {
                            JSONObject jsonObject = JSON.parseObject(jsonString);

                            for(PathPoint ppoint : JSON.parseArray(jsonObject.getString("pathList"), PathPoint.class)) {

                                if (!ppoint.getStakeId().isEmpty()) {
                                    Integer ot = ppoint.getOriginalType();
                                    Integer vt = ppoint.getVehicleType();
                                    // 表中没有的车型就舍弃掉
                                    if(ot != null) {
                                        if (!(ot == 1 || ot == 3 || ot == 7) && !(ot == 2 || ot == 8 || ot == 10 || ot == 11 ) && !(ot >= 170 && ot <= 183))
                                            continue;
                                    }
                                    else {
                                        if(!(vt >= 1 && vt <= 4) && !(vt >= 11 && vt <= 16))
                                            continue;
                                    }
                                    ppoint.setTimeStamp(jsonObject.getString("timeStamp"));
                                    out.collect(ppoint);
                                }
                            }

                        } catch (Exception e) {
                            System.err.println("解析 JSON 时出错: " + e.getMessage());
                        }
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<PathPoint>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<PathPoint>() {
                                                   @Override
                                                   public long extractTimestamp(PathPoint pathPoint, long recordTimestamp) {
                                                       return convertToTimestampMillis(pathPoint.getTimeStamp());
                                                   }
                                               }
                        ).withIdleness(Duration.ofSeconds(30))); // 超过30s不更新则标记为空闲分区;;

//        flatMapStream.print();

        // 按 rowkey 分组并处理
        DataStream<VehicleSegAccumulator> processedStream = flatMapStream.keyBy(ppoint -> ppoint.getStakeId().split("\\+")[0])
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.minutes(1)))
                .aggregate(new AggregateFunction<PathPoint, VehicleSegAccumulator, VehicleSegAccumulator>() {
                    @Override
                    public VehicleSegAccumulator createAccumulator() {
                        return new VehicleSegAccumulator(0L, 0, new HashMap<>(), new HashMap<>());
                    }

                    @Override
                    public VehicleSegAccumulator add(PathPoint ppoint, VehicleSegAccumulator vehicleSegAcc) {
                        if(vehicleSegAcc.getTimeStamp().equals(0L))
                            vehicleSegAcc.setTimeStamp(convertToTimestampMillis(ppoint.getTimeStamp()) / 60000 * 60000);
                        if(vehicleSegAcc.getStakeNum().equals(0))
                            vehicleSegAcc.setStakeNum(Integer.parseInt(ppoint.getStakeId().split("K")[1].split("\\+")[0]));

                        Map<Long, VehicleSeg> vehicleSegMap = new HashMap<>();
                        if(ppoint.getDirection() == 1)
                            vehicleSegMap = vehicleSegAcc.getVehicleSegMapD1();
                        else if(ppoint.getDirection() == 2)
                            vehicleSegMap = vehicleSegAcc.getVehicleSegMapD2();

                        VehicleSeg vehicleSeg;
                        if(!vehicleSegMap.containsKey(ppoint.getId())) {
                            vehicleSeg = new VehicleSeg(ppoint.getPlateNo(), ppoint.getId(), ppoint.getSpeed(), ppoint.getSpeed(), 1, ppoint.getVehicleType(), ppoint.getSpecialFlag());
                            vehicleSegMap.put(ppoint.getId(), vehicleSeg);
                        }
                        else {
                            // 更新vehicleSeg的speedSum和pointSum
                            vehicleSeg = vehicleSegMap.get(ppoint.getId());
                            vehicleSeg.setSpeedSum(vehicleSeg.getSpeedSum() + ppoint.getSpeed());
                            vehicleSeg.setPointSum(vehicleSeg.getPointSum() + 1);
                            vehicleSeg.setAveSpeed(vehicleSeg.getSpeedSum()/vehicleSeg.getPointSum());
                        }
                        return vehicleSegAcc;
                    }

                    @Override
                    public VehicleSegAccumulator getResult(VehicleSegAccumulator vehicleSegAcc) {
                        if (!vehicleSegAcc.getVehicleSegMapD1().isEmpty() || !vehicleSegAcc.getVehicleSegMapD2().isEmpty())
                            return vehicleSegAcc;
                        // 若出现异常（vehicleSegAcc为空），返回一个空的VehicleSegAccumulator
                        return new VehicleSegAccumulator();
                    }

                    @Override
                    public VehicleSegAccumulator merge(VehicleSegAccumulator a, VehicleSegAccumulator b) {
                        return new VehicleSegAccumulator();
                    }
                }).returns(VehicleSegAccumulator.class); // 显式指定输出类型;

//        processedStream.print();

        // 添加 Sink（终端操作）
        processedStream.addSink(new DynamicHBaseSink("JTSTCar", "cf"));

        env.execute("Flink JTSTCar to HBase");
    }

//     HBase Sink 实现
    public static class DynamicHBaseSink extends RichSinkFunction<VehicleSegAccumulator> {
        private final String baseTableName;
        private final String columnFamily;

        private transient org.apache.hadoop.conf.Configuration hadoopConf;
        private transient Connection hbaseConnection;
        private transient Table hbaseTable;

        private transient String currentTableName;
        private transient Long nextTableSwitchTime;
        private final ReentrantLock tableLock = new ReentrantLock();
        private static final ConcurrentHashMap<String, Object> tableCreationLocks = new ConcurrentHashMap<>();

        public DynamicHBaseSink(String baseTableName, String columnFamily) {
            this.baseTableName = baseTableName;
            this.columnFamily = columnFamily;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            hadoopConf = HBaseConfiguration.create();
            hadoopConf.set("hbase.zookeeper.quorum", "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142");
            hadoopConf.set("hbase.zookeeper.property.clientPort", "2181");
            hadoopConf.set("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", "400");
            hadoopConf.set("fs.defaultFS", "hdfs://100.65.38.139:9000");
            hadoopConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            hbaseConnection = ConnectionFactory.createConnection(hadoopConf);

            currentTableName = null;
            nextTableSwitchTime = null;
        }

        @Override
        public void invoke(VehicleSegAccumulator value, Context context) throws Exception {
            tableLock.lock();
            try {
                long rowKeyTime = value.getTimeStamp();
                int stakeNum = value.getStakeNum();
//                System.out.println("Sink Invoke - RowKey: " + value.f0 + ", Car Numbers: " + value.f1);

                if (currentTableName == null || isTimeToSwitch(rowKeyTime)) {
                    switchTable(rowKeyTime);
                }

                byte[] rowKey = Bytes.add(Bytes.toBytes(rowKeyTime), Bytes.toBytes(stakeNum));
                Put put = new Put(rowKey);
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("VehicleSegments"), Bytes.toBytes(JSON.toJSONString(value)));
                hbaseTable.put(put);
            } finally {
                tableLock.unlock();
            }
        }

        @Override
        public void close() throws Exception {
            if (hbaseTable != null) {
                hbaseTable.close();
            }
            if (hbaseConnection != null) {
                hbaseConnection.close();
            }
        }

        private boolean isTimeToSwitch(long rowKeyTime) {
            // 检测是否为下一小时
            return rowKeyTime >= nextTableSwitchTime;
        }

        private void switchTable(long rowKeyTime) throws Exception {
            tableLock.lock();
            try {
                currentTableName = baseTableName + "_" + getQuarterTableName(rowKeyTime);

                createTableIfNotExists(currentTableName, columnFamily);

                if (hbaseTable != null) {
                    hbaseTable.close();
                }

                hbaseTable = hbaseConnection.getTable(TableName.valueOf(currentTableName));

                nextTableSwitchTime = getNextQuarterStart(rowKeyTime);

                System.out.printf("切换到新表: %s，下一次切换时间: %s%n", currentTableName, getQuarterTableName(nextTableSwitchTime));
            } finally {
                tableLock.unlock();
            }
        }

        private void createTableIfNotExists(String tableName, String columnFamily) {
            tableLock.lock();
            try (Admin admin = hbaseConnection.getAdmin()) {
                TableName hbaseTableName = TableName.valueOf(tableName);

                Object lock = tableCreationLocks.computeIfAbsent(tableName, k -> new Object());

                synchronized (lock) {
                    // 获取最新的表列表
                    admin.listTables();
                    if (!admin.tableExists(hbaseTableName)) {
                        HTableDescriptor tableDescriptor = new HTableDescriptor(hbaseTableName);
                        tableDescriptor.addFamily(new HColumnDescriptor(columnFamily));
                        try {
                            admin.createTable(tableDescriptor);
                            System.out.println("Table created: " + tableName);
                        } catch (TableExistsException e) {
                            System.out.println("Table already exists, but not detected by tableExists(): " + tableName);
                        }
                    } else {
                        System.out.println("Table already exists: " + tableName);
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                tableLock.unlock();
            }
        }
    }

    public static long getNextQuarterStart(long timestampMs) {
        // 使用东八区
        ZoneId zone = ZoneId.of("Asia/Shanghai");
        Instant instant = Instant.ofEpochMilli(timestampMs);
        LocalDateTime dt = LocalDateTime.ofInstant(instant, zone);

        int month = dt.getMonthValue();
        int nextQuarterMonth = (month - 1) / 3 * 3 + 4;
        int year = dt.getYear();
        if (nextQuarterMonth > 12) {
            nextQuarterMonth = 1;
            year += 1;
        }

        LocalDateTime nextQuarterStart = LocalDateTime.of(year, nextQuarterMonth, 1, 0, 0, 0);
        ZonedDateTime zdt = nextQuarterStart.atZone(zone);
        return zdt.toInstant().toEpochMilli();
    }

    public static String getQuarterTableName(long timestampMs) {
        // 使用东八区（北京时间）
        ZoneId zone = ZoneId.of("Asia/Shanghai");
        Instant instant = Instant.ofEpochMilli(timestampMs);
        LocalDateTime dt = LocalDateTime.ofInstant(instant, zone);

        int year = dt.getYear();
        int month = dt.getMonthValue(); // 1~12
        int quarter = (month - 1) / 3 + 1; // 1~4

        return String.format("%dQ%d", year, quarter);
    }

    /**
     *  VehicleSeg 表示一辆车某分钟内监测的信息
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    @Setter
    public static class VehicleSeg {
        private String plateNo;
        private Long carId;
        private Float speedSum;
        // 新增平均速度
        private Float aveSpeed;
        private Integer pointSum;
        private Integer originalType = null;
        private String specialFlag = null;
    }

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class VehicleSegAccumulator {
        private Long timeStamp;
        private Integer stakeNum;
        // direction = 1
        private Map<Long, VehicleSeg> vehicleSegMapD1;
        // direction = 2
        private Map<Long, VehicleSeg> vehicleSegMapD2;
    }
}
