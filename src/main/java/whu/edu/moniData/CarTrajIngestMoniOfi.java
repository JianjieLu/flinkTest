package whu.edu.moniData;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class CarTrajIngestMoniOfi {
    private static final Logger logger = LogManager.getLogger(CarTrajIngestMoniOfi.class);
    public static void main(String[] args) throws Exception {

        // 设置 Flink 流执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        logger.info("Flink CarTraj 数据接入任务启动...");

        String brokers = "100.65.38.40:9092";
        String groupId = "flink-group";
//        List<String> topics = Arrays.asList("MergedPathData",
//                "MergedPathData.sceneTest.2", "MergedPathData.sceneTest.3",
//                "MergedPathData.sceneTest.4", "MergedPathData.sceneTest.5");
        List<String> topics = Arrays.asList("fiberData1",
                "fiberData2", "fiberData3",
                "fiberData4", "fiberData5",
                "fiberData6", "fiberData7",
                "fiberData8", "fiberData9",
                "fiberData10", "fiberData11");
        // 初始化第一个KafkaSource
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(topics.get(0))
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setProperty("auto.offset.commit", "true")
                .setProperty("consumer.max.poll.interval.ms",String.valueOf( 24*60*60*1000))
                .setProperty("session.timeout.ms",String.valueOf(24*60*60*1000))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> unionStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source 1");
        // 合并其他主题数据流
        for (int i = 1; i < topics.size(); i++) {
            KafkaSource<String> source = KafkaSource.<String>builder()
                    .setBootstrapServers(brokers)
                    .setTopics(topics.get(i))
                    .setGroupId(groupId)
                    .setStartingOffsets(OffsetsInitializer.latest())
                    .setProperty("auto.offset.commit", "true")
                    .setValueOnlyDeserializer(new SimpleStringSchema())
                    .setProperty("consumer.max.poll.interval.ms",String.valueOf( 24*60*60*1000))
                    .setProperty("session.timeout.ms",String.valueOf(24*60*60*1000))
                    .build();

            DataStream<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source " + (i + 1));
            unionStream = unionStream.union(stream);
        }

        unionStream
                .flatMap(new FlatMapFunction<String, Tuple4<String, Integer, Long, List<Tuple5<Double, Double, Integer, Integer, Double>>>>() {
                    private final Map<String, List<Tuple5<Double,Double, Integer, Integer, Double>>> map = new LinkedHashMap<>();
                    private final Map<String, Long> mapJudge = new HashMap<>();
                    private final Map<String, String> mapTimeSeg = new HashMap<>();
                    private final Map<String, Integer> mapType = new HashMap<>();
                    private final Map<String, Long> mapStartTime = new HashMap<>();

                    @Override
                    public void flatMap(String jsonString, Collector<Tuple4<String, Integer, Long, List<Tuple5<Double,Double, Integer, Integer, Double>>>> out) {
                        try {
                            Map<String, Long> temp = new HashMap<>();
                            JSONObject jsonObject = new JSONObject(jsonString);
                            String timeStampStr = jsonObject.getString("timeStamp");
                            long timeObs;
                            try {
                                // 尝试按三位毫秒格式解析
                                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS");
                                LocalDateTime localDateTime = LocalDateTime.parse(timeStampStr, formatter);
                                timeObs = localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
                            } catch (Exception e) {
                                // 若三位毫秒格式解析失败，尝试按两位毫秒格式解析
                                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SS");
                                LocalDateTime localDateTime = LocalDateTime.parse(timeStampStr, formatter);
                                timeObs = localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
                            }

                            JSONArray tdataArray = jsonObject.getJSONArray("pathList");
                            int type;
                            for (int i = 0; i < tdataArray.length(); i++) {
                                JSONObject tdataObject = tdataArray.getJSONObject(i);
                                String carNumber = tdataObject.getString("plateNo");
                                temp.put(carNumber, timeObs);
                                mapJudge.put(carNumber, timeObs);
                                if (map.get(carNumber) == null) {
                                    type = tdataObject.getInt("vehicleType");
                                    mapTimeSeg.put(carNumber, timeObs + "-" + carNumber);
                                    mapType.put(carNumber, type);
                                    mapStartTime.put(carNumber, timeObs);
                                    List<Tuple5<Double,Double, Integer, Integer, Double>> list = new ArrayList<>();
                                    int direction;
                                    try {
                                        direction = tdataObject.getInt("direction");
                                    } catch (JSONException e) {
                                        // 如果找不到direction字段，给予默认值
                                        direction = -1;
                                    }
                                    list.add(new Tuple5<>(tdataObject.getDouble("longitude"),tdataObject.getDouble("latitude"),
                                            tdataObject.getInt("laneNo"), direction,
                                            tdataObject.getDouble("speed")));
                                    map.put(carNumber, list);
                                    logger.debug("新建车辆轨迹缓存: 车牌号="+carNumber+", 时间="+ timeStampStr);
                                } else {
                                    List<Tuple5<Double,Double, Integer, Integer, Double>> list = map.get(carNumber);
                                    int direction;
                                    try {
                                        direction = tdataObject.getInt("direction");
                                    } catch (JSONException e) {
                                        // 如果找不到direction字段，给予默认值
                                        direction = -1;
                                    }
                                    list.add(new Tuple5<>(tdataObject.getDouble("longitude"),tdataObject.getDouble("latitude"),
                                            tdataObject.getInt("laneNo"), direction,
                                            tdataObject.getDouble("speed")));
                                }
                            }

                            Map<String, Long> temp1 = new HashMap<>(mapJudge);
                            temp1.keySet().removeAll(temp.keySet());
                            if (!temp1.isEmpty()) {
                                for (String key : temp1.keySet()) {
                                    out.collect(new Tuple4<>(
                                            mapTimeSeg.get(key),
                                            mapType.get(key),
                                            mapJudge.get(key),
                                            map.get(key)
                                    ));
                                }
                            }

                            mapJudge.putAll(temp);
                        } catch (Exception e) {
                            System.err.println("解析 JSON 时出错: " + e.getMessage());
                        }
                    }
                })
                .addSink(new DynamicHBaseSink("CarTraj", "cf0","1h"));
//                .addSink(new SinkFunction<Tuple4<String, Integer, Long, List<Tuple5<Double,Double, Integer, Integer, Double>>>>() {
//                    @Override
//                    public void invoke(Tuple4<String, Integer, Long, List<Tuple5<Double, Double, Integer, Integer, Double>>> value, Context context) {
//                        System.out.println("RowKey: " + value.f0);
//                        System.out.println("Type: " + value.f1);
//                        System.out.println("Latest Time: " + value.f2);
//                        System.out.println("Trajectory: " + value.f3);
//                        System.out.println("----------------------");
//                    }
//                });

        env.execute("Flink CarTraj to HBase");
    }

    private static class DynamicHBaseSink extends RichSinkFunction<Tuple4<String, Integer, Long, List<Tuple5<Double, Double, Integer, Integer, Double>>>> {
        private final String baseTableName;
        private final String columnFamily;
        private final long intervalSeconds; // 表切换间隔（秒）

        private transient Configuration conf;
        private transient Connection connection;
        private transient Table table;

        private transient String currentTableName;
        private transient LocalDateTime nextTableSwitchTime;
        private final ReentrantLock tableLock = new ReentrantLock();
        private static final ConcurrentHashMap<String, Object> tableCreationLocks = new ConcurrentHashMap<>();

        public DynamicHBaseSink(String baseTableName, String columnFamily, String interval) {
            this.baseTableName = baseTableName;
            this.columnFamily = columnFamily;
            this.intervalSeconds = parseToSec(interval);
        }

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            super.open(parameters);
            conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142,100.65.38.36,100.65.38.37,100.65.38.38");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            conf.set("zookeeper.session.timeout", "120000");
//            conf.set("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", "400");
            conf.set("fs.defaultFS", "hdfs://100.65.38.139:9000");
            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            connection = ConnectionFactory.createConnection(conf);
            // 初始化为 null，等待第一条数据来确定表名
            currentTableName = null;
            nextTableSwitchTime = null;
        }

        @Override
        public void invoke(Tuple4<String, Integer, Long, List<Tuple5<Double, Double, Integer, Integer, Double>>> value, Context context) throws Exception {
            tableLock.lock();
            try {
                long rowKeyTime = value.f2;
                // 切换表（如果必要）
                if (currentTableName == null || isTimeToSwitch(rowKeyTime)) {
                    switchTable(rowKeyTime);
                }
                // 插入数据
                String rowKey = value.f0;
                Put put = new Put(Bytes.toBytes(rowKey)); // rowKey 使用时间段
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("type"), Bytes.toBytes(value.f1.toString()));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("latest_time"), Bytes.toBytes(value.f2.toString()));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("trajectory"), Bytes.toBytes(value.f3.toString()));
                table.put(put);
            } finally {
                tableLock.unlock();
            }

        }

        @Override
        public void close() throws Exception {
            if (table != null) {
                table.close();
            }
            if (connection != null) {
                connection.close();
            }
        }

        private boolean isTimeToSwitch(long rowKeyTime) {
            LocalDateTime rowKeyDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(rowKeyTime), ZoneId.systemDefault());
            return rowKeyDateTime.isAfter(nextTableSwitchTime);
        }

        private void switchTable(long rowKeyTime) throws IOException {
            tableLock.lock();
            try {
                LocalDateTime rowKeyDateTime = LocalDateTime.ofInstant(
                        Instant.ofEpochMilli(rowKeyTime), ZoneId.systemDefault()
                );

                // 对齐到整点小时（关键修改点）
                LocalDateTime alignedTime = rowKeyDateTime.truncatedTo(ChronoUnit.HOURS);
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd_HH");
                currentTableName = baseTableName + "_" + alignedTime.format(formatter);

                // 统一修改两处 nextTableSwitchTime 的赋值逻辑
                if (nextTableSwitchTime == null) {
                    nextTableSwitchTime = alignedTime.plusHours(1); // 初始化时对齐到整点
                } else {
                    nextTableSwitchTime = alignedTime.plusHours(1); // 后续切换时同样对齐
                }

                // 创建表并切换
                createTableIfNotExists(currentTableName, columnFamily);
                if (table != null) table.close();
                table = connection.getTable(TableName.valueOf(currentTableName));

                System.out.printf("切换到新表: %s，下一次切换时间: %s%n",
                        currentTableName, nextTableSwitchTime.format(formatter));
            } finally {
                tableLock.unlock();
            }
        }

        public void createTableIfNotExists(String tableName, String columnFamily) throws IOException {
            tableLock.lock(); // 确保线程安全
            try (Admin admin = connection.getAdmin()) { // 使用 HBase 连接创建 Admin
                TableName hbaseTableName = TableName.valueOf(tableName);

                // 使用 ConcurrentHashMap 来控制是否正在创建表
                Object lock = tableCreationLocks.computeIfAbsent(tableName, k -> new Object());

                synchronized (lock) {
                    // 检查表是否已经存在
                    if (!admin.tableExists(hbaseTableName)) {
                        // 如果表不存在，创建新表
                        HTableDescriptor tableDescriptor = new HTableDescriptor(hbaseTableName);
                        tableDescriptor.addFamily(new HColumnDescriptor(columnFamily));
                        admin.createTable(tableDescriptor); // 创建表
                        logger.info("Table created: " + tableName);
                    } else {
                        // 如果表已存在，打印表已存在的消息
                        System.out.println("Table already exists: " + tableName);
                    }
                }
            } finally {
                tableLock.unlock(); // 释放锁
            }
        }


        private long parseToSec(String timeStr) {
            if (timeStr.endsWith("s")) {
                return Long.parseLong(timeStr.replace("s", ""));
            } else if (timeStr.endsWith("m")) {
                return Long.parseLong(timeStr.replace("m", "")) * 60;
            } else if (timeStr.endsWith("h")) {
                return Long.parseLong(timeStr.replace("h", "")) * 3600;
            } else if (timeStr.endsWith("d")) {
                return Long.parseLong(timeStr.replace("d", "")) * 86400;
            } else {
                throw new IllegalArgumentException("Invalid interval format: " + timeStr);
            }
        }
    }
}