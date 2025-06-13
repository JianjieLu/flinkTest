package whu.edu.moniData.ingest;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
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
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

public class CarTrajIngestMoniOfi {
    private static final Logger logger = LogManager.getLogger(CarTrajIngestMoniOfi.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        logger.info("Flink CarTraj 数据接入任务启动...");

        String brokers = "100.65.38.40:9092";
        String groupId = "flink-group";
        List<String> topics = Arrays.asList(
                "fiberData1", "fiberData2", "fiberData3",
                "fiberData4", "fiberData5", "fiberData6",
                "fiberData7", "fiberData8", "fiberData9",
                "fiberData10", "fiberData11"
        );

        // 初始化第一个KafkaSource
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(topics.get(0))
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setProperty("auto.offset.commit", "true")
                .setProperty("consumer.max.poll.interval.ms", String.valueOf(24 * 60 * 60 * 1000))
                .setProperty("session.timeout.ms", String.valueOf(24 * 60 * 60 * 1000))
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
                    .setProperty("consumer.max.poll.interval.ms", String.valueOf(24 * 60 * 60 * 1000))
                    .setProperty("session.timeout.ms", String.valueOf(24 * 60 * 60 * 1000))
                    .build();

            DataStream<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source " + (i + 1));
            unionStream = unionStream.union(stream);
        }

        unionStream
                .flatMap(new FlatMapFunction<String, Tuple4<String, Integer, Long, List<Tuple5<Double, Double, Integer, Integer, Double>>>>() {
                    private final Map<String, List<Tuple5<Double, Double, Integer, Integer, Double>>> map = new HashMap<>();
                    private final Map<String, Long> mapJudge = new HashMap<>();
                    private final Map<String, String> mapTimeSeg = new HashMap<>();
                    private final Map<String, Integer> mapType = new HashMap<>();
                    private final Map<String, Long> mapStartTime = new HashMap<>();

                    @Override
                    public void flatMap(String jsonString, Collector<Tuple4<String, Integer, Long, List<Tuple5<Double, Double, Integer, Integer, Double>>>> out) {
                        try {
                            Map<String, Long> temp = new HashMap<>();
                            JSONObject jsonObject = new JSONObject(jsonString);
                            String timeStampStr = jsonObject.getString("timeStamp");
                            long timeObs;
                            try {
                                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS");
                                LocalDateTime localDateTime = LocalDateTime.parse(timeStampStr, formatter);
                                timeObs = localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
                            } catch (Exception e) {
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
                                if (!map.containsKey(carNumber)) {
                                    type = tdataObject.getInt("vehicleType");
                                    mapTimeSeg.put(carNumber, timeObs + "-" + carNumber);
                                    mapType.put(carNumber, type);
                                    mapStartTime.put(carNumber, timeObs);
                                    List<Tuple5<Double, Double, Integer, Integer, Double>> list = new ArrayList<>();
                                    int direction;
                                    try {
                                        direction = tdataObject.getInt("direction");
                                    } catch (JSONException e) {
                                        direction = -1;
                                    }
                                    list.add(new Tuple5<>(tdataObject.getDouble("longitude"),
                                            tdataObject.getDouble("latitude"),
                                            tdataObject.getInt("laneNo"),
                                            direction,
                                            tdataObject.getDouble("speed")));
                                    map.put(carNumber, list);
                                    logger.debug("新建车辆轨迹缓存: 车牌号=" + carNumber + ", 时间=" + timeStampStr);
                                } else {
                                    List<Tuple5<Double, Double, Integer, Integer, Double>> list = map.get(carNumber);
                                    int direction;
                                    try {
                                        direction = tdataObject.getInt("direction");
                                    } catch (JSONException e) {
                                        direction = -1;
                                    }
                                    list.add(new Tuple5<>(tdataObject.getDouble("longitude"),
                                            tdataObject.getDouble("latitude"),
                                            tdataObject.getInt("laneNo"),
                                            direction,
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
                                    map.remove(key);
                                    mapTimeSeg.remove(key);
                                    mapType.remove(key);
                                    mapStartTime.remove(key);
                                }
                            }

                            mapJudge.putAll(temp);
                        } catch (Exception e) {
                            System.err.println("解析 JSON 时出错: " + e.getMessage());
                            e.printStackTrace();
                        }
                    }
                })
                .addSink(new DailyHBaseSink("CarTraj", "cf0"));

        env.execute("Flink CarTraj to HBase");
    }

    private static class DailyHBaseSink extends RichSinkFunction<Tuple4<String, Integer, Long, List<Tuple5<Double, Double, Integer, Integer, Double>>>> {
        private final String baseTableName;
        private final String columnFamily;
        private transient Configuration conf;
        private transient Connection connection;
        private final ReentrantLock tableLock = new ReentrantLock();
        private final Map<String, Table> tableCache = new HashMap<>();
        private static final Map<String, Object> tableCreationLocks = new HashMap<>();

        public DailyHBaseSink(String baseTableName, String columnFamily) {
            this.baseTableName = baseTableName;
            this.columnFamily = columnFamily;
        }

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            super.open(parameters);
            conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142,100.65.38.36,100.65.38.37,100.65.38.38");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            conf.set("zookeeper.session.timeout", "120000");
            conf.set("fs.defaultFS", "hdfs://100.65.38.139:9000");
            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            connection = ConnectionFactory.createConnection(conf);
        }

        @Override
        public void invoke(Tuple4<String, Integer, Long, List<Tuple5<Double, Double, Integer, Integer, Double>>> value,
                           Context context) throws Exception {
            String rowKey = value.f0;
            String tableName = getTableNameFromRowKey(rowKey);
            Table table = getOrCreateTable(tableName);

            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("type"),
                    Bytes.toBytes(String.valueOf(value.f1)));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("latest_time"),
                    Bytes.toBytes(String.valueOf(value.f2)));

            // 构建轨迹JSON
            JSONArray trajectoryJson = new JSONArray();
            for (Tuple5<Double, Double, Integer, Integer, Double> point : value.f3) {
                JSONObject pointJson = new JSONObject();
                pointJson.put("longitude", point.f0);
                pointJson.put("latitude", point.f1);
                pointJson.put("laneNo", point.f2);
                pointJson.put("direction", point.f3);
                pointJson.put("speed", point.f4);
                trajectoryJson.put(pointJson);
            }

            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("trajectory"),
                    Bytes.toBytes(trajectoryJson.toString()));

            table.put(put);
            logger.info("成功写入轨迹数据: " + rowKey);
        }

        private String getTableNameFromRowKey(String rowKey) {
            String[] parts = rowKey.split("-");
            if (parts.length < 1) {
                throw new IllegalArgumentException("Invalid rowKey: " + rowKey);
            }
            long startTime = Long.parseLong(parts[0]);
            return getDailyTableName(startTime);
        }

        private String getDailyTableName(long timestamp) {
            LocalDate date = LocalDateTime.ofInstant(
                    Instant.ofEpochMilli(timestamp), ZoneId.systemDefault()).toLocalDate();
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
            return baseTableName + "_" + date.format(formatter);
        }

        private Table getOrCreateTable(String tableName) throws IOException {
            tableLock.lock();
            try {
                Table table = tableCache.get(tableName);
                if (table == null) {
                    createTableIfNotExists(tableName);
                    table = connection.getTable(TableName.valueOf(tableName));
                    tableCache.put(tableName, table);
                    logger.info("已打开表: " + tableName);
                }
                return table;
            } finally {
                tableLock.unlock();
            }
        }

        private void createTableIfNotExists(String tableName) throws IOException {
            Object lock = tableCreationLocks.computeIfAbsent(tableName, k -> new Object());
            synchronized (lock) {
                try (Admin admin = connection.getAdmin()) {
                    TableName hbaseTableName = TableName.valueOf(tableName);
                    if (!admin.tableExists(hbaseTableName)) {
                        HTableDescriptor tableDescriptor = new HTableDescriptor(hbaseTableName);
                        tableDescriptor.addFamily(new HColumnDescriptor(columnFamily));
                        admin.createTable(tableDescriptor);
                        logger.info("已创建表: " + tableName);
                    } else {
                        logger.info("表已存在: " + tableName);
                    }
                }
            }
        }

        @Override
        public void close() throws Exception {
            // 锁的正确使用方式：lock() + try + finally unlock()
            tableLock.lock();
            try {
                // 关闭所有缓存的表
                for (Map.Entry<String, Table> entry : tableCache.entrySet()) {
                    if (entry.getValue() != null) {
                        try {
                            entry.getValue().close();
                        } catch (Exception e) {
                            logger.error("关闭表连接错误: " + entry.getKey(), e);
                        }
                    }
                }
                tableCache.clear();

                // 关闭HBase连接
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (Exception e) {
                        logger.error("关闭HBase连接错误", e);
                    }
                }
            } finally {
                tableLock.unlock(); // 确保锁被释放
            }
        }
    }
}