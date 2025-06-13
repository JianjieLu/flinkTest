package whu.edu.moniData.ingest;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;

import whu.edu.ljj.flink.xiaohanying.Utils.PathPoint1;
import lombok.*;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import static whu.edu.ljj.flink.xiaohanying.Utils.convertToTimestampMillis;

/**
 * SegCarIngestMoniOfiV6
 * 存储匝道数据
 * 更新存储方式->之前可能会出现跨分钟统计的现象，这里采用和计算拥堵一样的方式。
 * 5.4-5.5
 *  a. 改用窗口
 *  b. 暂时去掉MergedPathData，等待基站数据解析好后再接入
 *  c.(难点) 实现Redis集群模式的实时数据缓存（热数据）
 *  5.12
 *  a. 接入真实基站数据
 */
public class SegCarIngestMoniOfiV6 {
    // 定义默认值常量
    private static final Integer DEFAULT_PLATE_COLOR = 0;
    private static final Integer DEFAULT_ORIGINAL_TYPE = 0;
    private static final Integer DEFAULT_ORIGINAL_COLOR = 0;
    private static final Double DEFAULT_CAR_ANGLE = 0.0;
    private static final String DEFAULT_SPECIAL_FLAG = "N/A";
    public static void main(String[] args) throws Exception {
        // 设置 Flink 流执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // 配置 KafkaSource
        String brokers = "100.65.38.139:9092";
        String groupId = "flink-group-SegCar"; // 消费者组ID

        // 主题列表
//        List<String> topics = Arrays.asList(
//                "MergedPathData",
//                "MergedPathData.sceneTest.1",
//                "MergedPathData.sceneTest.2",
//                "MergedPathData.sceneTest.3",
//                "MergedPathData.sceneTest.4",
//                "MergedPathData.sceneTest.5");
        List<String> topics = Arrays.asList("fiberData1",
                "fiberData2", "fiberData3",
                "fiberData4", "fiberData5",
                "fiberData6", "fiberData7",
                "fiberData8", "fiberData9",
                "fiberData10", "fiberData11");
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

        DataStream<PathPoint1> flatMapStream = unionStream
                .flatMap(new FlatMapFunction<String, PathPoint1>() {


                    @Override
                    public void flatMap(String jsonString, Collector<PathPoint1> out) {
                        try {
                            JSONObject jsonObject = JSON.parseObject(jsonString);

                            // 1. 统一时间戳处理 ----------------------------------------------
                            final String formattedTime = processTimestamp(jsonObject);

                            // 2. 处理路径点列表 ----------------------------------------------
                            JSONArray pathList = jsonObject.getJSONArray("pathList");
                            for (int i = 0; i < pathList.size(); i++) {
                                JSONObject pathObj = pathList.getJSONObject(i);
                                // 3. 创建PathPoint并填充数据 --------------------------------
                                PathPoint1 ppoint = new PathPoint1();
                                // 必填字段（JSON中存在的）
                                ppoint.setId(pathObj.getLong("id"));
                                ppoint.setPlateNo(pathObj.getString("plateNo"));
                                ppoint.setSpeed(pathObj.getFloat("speed"));
                                ppoint.setLaneNo(pathObj.getInteger("laneNo"));
                                ppoint.setMileage(pathObj.getInteger("mileage"));
                                ppoint.setDirection(pathObj.getInteger("direction"));
                                ppoint.setStakeId(pathObj.getString("stakeId"));
                                ppoint.setLongitude(pathObj.getDouble("longitude"));
                                ppoint.setLatitude(pathObj.getDouble("latitude"));
                                // 4. 设置统一的时间戳字段
                                ppoint.setTimeStamp(formattedTime);

                                    out.collect(ppoint);

                            }
                        } catch (Exception e) {
                            handleError(jsonString, e);
                        }
                    }

                    // 统一时间处理逻辑
                    private String processTimestamp(JSONObject jsonObject) throws DateTimeParseException {
                        String rawTimestamp = jsonObject.getString("timeStamp");

                        // 尝试多种时间格式解析
                        LocalDateTime parsedTime = tryParseTimestamp(rawTimestamp);

                        // 统一格式化为: yyyy-MM-dd HH:mm:ss:SSS
                        return parsedTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS"));
                    }

                    // 时间解析方法（支持三种格式）
                    private LocalDateTime tryParseTimestamp(String timestamp) throws DateTimeParseException {
                        List<DateTimeFormatter> formatters = Arrays.asList(
                                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS"),
                                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SS"),
                                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                        );

                        for (DateTimeFormatter formatter : formatters) {
                            try {
                                return LocalDateTime.parse(timestamp, formatter);
                            } catch (DateTimeParseException ignored) {
                                // 尝试下一种格式
                            }
                        }
                        throw new DateTimeParseException("无法解析时间格式", timestamp, 0);
                    }

                    // 处理缺失字段的默认值
                    private void handleMissingFields(PathPoint1 ppoint) {
                        // 处理数字类型字段
                        if (ppoint.getPlateColor() == null) {
                            ppoint.setPlateColor(DEFAULT_PLATE_COLOR);
                        }
                        if (ppoint.getOriginalType() == null) {
                            ppoint.setOriginalType(DEFAULT_ORIGINAL_TYPE);
                        }
                        if (ppoint.getOriginalColor() == null) {
                            ppoint.setOriginalColor(DEFAULT_ORIGINAL_COLOR);
                        }

                        // 处理可能未正确映射的double字段


                        // 处理字符串字段
                        if (ppoint.getSpecialFlag() == null || ppoint.getSpecialFlag().isEmpty()) {
                            ppoint.setSpecialFlag(DEFAULT_SPECIAL_FLAG);
                        }
                    }

                    // 数据有效性验证
                    private boolean isValidPoint(PathPoint1 ppoint) {
                        // 必须字段检查
                        if (ppoint.getStakeId() == null || ppoint.getStakeId().trim().isEmpty()) {
                            return false;
                        }

                        // 坐标范围验证
                        if (Math.abs(ppoint.getLongitude()) > 180 || Math.abs(ppoint.getLatitude()) > 90) {
                            return false;
                        }

                        // 业务逻辑验证
                        if (ppoint.getSpeed() < 0 || ppoint.getSpeed() > 200) { // 假设最高时速200
                            return false;
                        }

                        return true;
                    }

                    // 错误处理增强
                    private void handleError(String jsonString, Exception e) {
                        String errorInfo = String.format(
                                "JSON处理失败！\n" +
                                        "原始数据: %s\n" +
                                        "错误类型: %s\n" +
                                        "错误信息: %s\n" +
                                        "StackTrace: %s",
                                jsonString,
                                e.getClass().getSimpleName(),
                                e.getMessage(),
                                Arrays.toString(e.getStackTrace())
                        );

                        System.err.println(errorInfo);
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<PathPoint1>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((event, timestamp) ->
                                        convertToTimestampMillis(event.getTimeStamp())
                                )
                                .withIdleness(Duration.ofSeconds(30))
                );

//        flatMapStream.print();

        // 按 rowkey 分组并处理
        DataStream<Tuple2<String, String>> processedStream = flatMapStream.keyBy(ppoint -> ppoint.getStakeId().split("\\+")[0])
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.minutes(1)))
                .aggregate(new AggregateFunction<PathPoint1, VehicleSegAccumulator, Tuple2<String, String>>() {
                    @Override
                    public VehicleSegAccumulator createAccumulator() {
                        Map<Long, VehicleSeg> vehicleSegMap = new HashMap<>();
                        return new VehicleSegAccumulator("", vehicleSegMap);
                    }

                    @Override
                    public VehicleSegAccumulator add(PathPoint1 ppoint, VehicleSegAccumulator vehicleSegAcc) {
                        vehicleSegAcc.setCurrentKey(convertToTimestampMillis(ppoint.getTimeStamp()) / 60000 * 60000 + "_" + ppoint.getStakeId().split("\\+")[0]);
                        Map<Long, VehicleSeg> vehicleSegMap = vehicleSegAcc.getVehicleSegMap();

                        if(!vehicleSegMap.containsKey(ppoint.getId())) {
                            VehicleSeg vehicleSeg = new VehicleSeg(ppoint.getPlateNo(), ppoint.getId(), ppoint.getSpeed(), ppoint.getDirection(), 1, ppoint.getOriginalType(), ppoint.getSpecialFlag());
                            vehicleSegMap.put(ppoint.getId(), vehicleSeg);
                        }
                        else {
                            // 更新vehicleSeg的speedSum和pointSum
                            VehicleSeg vehicleSeg = vehicleSegMap.get(ppoint.getId());
                            vehicleSeg.setSpeedSum(vehicleSeg.getSpeedSum() + ppoint.getSpeed());
                            vehicleSeg.setPointSum(vehicleSeg.getPointSum() + 1);
                            // 显式更新一下
                            vehicleSegMap.put(ppoint.getId(), vehicleSeg);
                        }
                        return vehicleSegAcc;
                    }

                    @Override
                    public Tuple2<String, String> getResult(VehicleSegAccumulator vehicleSegAcc) {
                        if (!vehicleSegAcc.getVehicleSegMap().isEmpty()) {
                            List<VehicleSeg> mergedVehicleSeg = new ArrayList<>(vehicleSegAcc.getVehicleSegMap().values());
                            return Tuple2.of(vehicleSegAcc.getCurrentKey(), JSON.toJSONString(mergedVehicleSeg));
                        }
                        // 若出现异常（vehicleSegAcc为空），返回一个空的Tuple2
                        return new Tuple2<>();
                    }

                    @Override
                    public VehicleSegAccumulator merge(VehicleSegAccumulator a, VehicleSegAccumulator b) {
                        return new VehicleSegAccumulator();
                    }
                }).returns(Types.TUPLE(Types.STRING, Types.STRING)); // 显式指定输出类型;

//        processedStream.print();

        // 添加 Sink（终端操作）
        processedStream.addSink(new DynamicHBaseSink("STCar", "cf"));

        env.execute("Flink STCar to HBase");
    }

//     HBase Sink 实现
    public static class DynamicHBaseSink extends RichSinkFunction<Tuple2<String, String>> {
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
            hadoopConf.set("hbase.zookeeper.quorum", "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.36,100.65.38.37,100.65.38.38");
            hadoopConf.set("hbase.zookeeper.property.clientPort", "2181");
            hadoopConf.set("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", "400");
            hadoopConf.set("fs.defaultFS", "hdfs://100.65.38.139:9000");
            hadoopConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            hbaseConnection = ConnectionFactory.createConnection(hadoopConf);

            currentTableName = null;
            nextTableSwitchTime = null;
        }

        @Override
        public void invoke(Tuple2<String, String> value, Context context) throws Exception {
            tableLock.lock();
            try {
                long rowKeyTime = Long.parseLong(value.f0.split("_")[0]);
//                System.out.println("Sink Invoke - RowKey: " + value.f0 + ", Car Numbers: " + value.f1);

                if (currentTableName == null || isTimeToSwitch(rowKeyTime)) {
                    switchTable(rowKeyTime);
                }

                String rowKey = value.f0;
                Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("VehicleSegments"), Bytes.toBytes(value.f1.toString()));
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
                if (nextTableSwitchTime == null) {
                    nextTableSwitchTime = (rowKeyTime / 1000 / 3600 * 3600 + 3600) * 1000;
                }

                String timestamp = convertToHBaseTableTime(rowKeyTime);
                currentTableName = baseTableName + "_" + timestamp;

                createTableIfNotExists(currentTableName, columnFamily);

                if (hbaseTable != null) {
                    hbaseTable.close();
                }
                hbaseTable = hbaseConnection.getTable(TableName.valueOf(currentTableName));

                nextTableSwitchTime = (rowKeyTime / 1000 / 3600 * 3600 + 3600) * 1000;

                System.out.printf("切换到新表: %s，下一次切换时间: %s%n", currentTableName, convertToHBaseTableTime(nextTableSwitchTime));
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

    public static String convertToHBaseTableTime(long timestamp) {
        // 定义日期时间格式
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd_HH");

        // 将时间戳转换为 Instant 对象
        Instant instant = Instant.ofEpochMilli(timestamp);

        // 将 Instant 转换为 LocalDateTime（考虑系统默认时区）
        LocalDateTime dateTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());

        // 格式化为字符串
        String dateTimeStr = dateTime.format(formatter);

        return dateTimeStr;
    }

    private String convertToThreeDigitMillis(String timeStampStr) {
        if (timeStampStr.contains(":")) {
            String[] parts = timeStampStr.split(":");
            if (parts.length == 5 && parts[4].length() == 2) { // 判断是否为两位毫秒格式
                // 在毫秒部分前面补零
                return parts[0] + ":" + parts[1] + ":" + parts[2] + ":" + parts[3] + ":" + String.format("%03d", Integer.parseInt(parts[4]));
            }
        }
        return timeStampStr; // 如果不是两位毫秒格式，直接返回原字符串
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
        private long carId;
        private float speedSum;
        private int direction;
        private int pointSum;
        private Integer originalType = null;
        private String specialFlag = null;
    }

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class VehicleSegAccumulator {
        private String currentKey;  // 存储键值
        //        private Map<Integer, Map<Long, VehicleSeg>> vehicleSegMap;
        private Map<Long, VehicleSeg> vehicleSegMap;
    }
}