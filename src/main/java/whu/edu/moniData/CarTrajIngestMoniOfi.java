package whu.edu.moniData;

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
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class CarTrajIngestMoniOfi {
    private static final Logger logger = LogManager.getLogger(CarTrajIngestMoniOfi.class);

    // 会话超时配置
    private static final long SESSION_TIMEOUT_MS = 5000; // 5秒会话超时
    private static final long SAMPLING_INTERVAL_MS = 5000; // 5秒采样间隔

    // 存储结构
    private static final Map<String, List<Tuple5<Double,Double, Integer, Integer, Double>>> map = new LinkedHashMap<>();
    private static final Map<String, String> mapTimeSeg = new HashMap<>();
    private static final Map<String, Integer> mapType = new HashMap<>();
    private static final Map<String, Long> lastSeenTime = new HashMap<>();
    private static final Map<String, Long> lastSampleTime = new HashMap<>(); // 新增：记录每辆车的最后采样时间

    // 辅助方法：安全获取方向值
    private static int getDirectionSafely(JSONObject tdataObject) {
        try {
            return tdataObject.getInt("direction");
        } catch (JSONException e) {
            return -1; // 默认值
        }
    }

    public static void main(String[] args) throws Exception {

        // 设置 Flink 流执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        logger.info("Flink CarTraj 数据接入任务启动...");

        String brokers = "100.65.38.40:9092";
        String groupId = "flink-group";
        List<String> topics = Arrays.asList("fiberData1", "fiberData2", "fiberData3",
                "fiberData4", "fiberData5", "fiberData6", "fiberData7",
                "fiberData8", "fiberData9", "fiberData10", "fiberData11");

        // 初始化第一个KafkaSource
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(topics.get(0))
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setProperty("auto.offset.commit", "true")
                .setProperty("consumer.max.poll.interval.ms",String.valueOf(24 * 60 * 60 * 1000))
                .setProperty("session.timeout.ms",String.valueOf(24 * 60 * 60 * 1000))
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
                    .setProperty("consumer.max.poll.interval.ms",String.valueOf(24 * 60 * 60 * 1000))
                    .setProperty("session.timeout.ms",String.valueOf(24 * 60 * 60 * 1000))
                    .build();

            DataStream<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source " + (i + 1));
            unionStream = unionStream.union(stream);
        }

        unionStream
                .flatMap(new FlatMapFunction<String, Tuple4<String, Integer, Long, List<Tuple5<Double, Double, Integer, Integer, Double>>>>() {

                    @Override
                    public void flatMap(String jsonString, Collector<Tuple4<String, Integer, Long, List<Tuple5<Double,Double, Integer, Integer, Double>>>> out) {
                        try {
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

                                // 更新最后看到时间
                                lastSeenTime.put(carNumber, timeObs);

                                // 获取最后采样时间（如果没有则为0）
                                long lastSample = lastSampleTime.getOrDefault(carNumber, 0L);

                                // 检查是否超过采样间隔
                                if (timeObs - lastSample >= SAMPLING_INTERVAL_MS) {
                                    if (map.get(carNumber) == null) {
                                        type = tdataObject.getInt("vehicleType");
                                        mapTimeSeg.put(carNumber, timeObs + "-" + carNumber);
                                        mapType.put(carNumber, type);
                                        List<Tuple5<Double,Double, Integer, Integer, Double>> list = new ArrayList<>();
                                        list.add(new Tuple5<>(tdataObject.getDouble("longitude"),
                                                tdataObject.getDouble("latitude"),
                                                tdataObject.getInt("laneNo"),
                                                getDirectionSafely(tdataObject),
                                                tdataObject.getDouble("speed")));
                                        map.put(carNumber, list);
                                        logger.debug("新建车辆轨迹缓存: 车牌号="+carNumber+", 时间="+ timeStampStr);
                                    } else {
                                        List<Tuple5<Double,Double, Integer, Integer, Double>> list = map.get(carNumber);
                                        list.add(new Tuple5<>(tdataObject.getDouble("longitude"),
                                                tdataObject.getDouble("latitude"),
                                                tdataObject.getInt("laneNo"),
                                                getDirectionSafely(tdataObject),
                                                tdataObject.getDouble("speed")));
                                    }

                                    // 更新最后采样时间
                                    lastSampleTime.put(carNumber, timeObs);
                                } else {
                                    logger.debug("跳过点: 车牌号=" + carNumber + ", 当前时间=" + timeStampStr +
                                            ", 上次采样时间=" + new Date(lastSample) +
                                            ", 间隔=" + (timeObs - lastSample) + "ms");
                                }
                            }

                            // 处理超时车辆
                            Set<String> timeoutCars = new HashSet<>();
                            for (Map.Entry<String, Long> entry : lastSeenTime.entrySet()) {
                                String carNum = entry.getKey();
                                long lastSeen = entry.getValue();
                                long de = timeObs - lastSeen;
                                if(1000>de&&de>0){
                                    System.out.println("超时小于1s:"+carNum);
                                    System.out.println(lastSeenTime);
                                }
                                if(2000>de&&de>1000){
                                    System.out.println("超时小于2s:"+carNum);
                                    System.out.println(lastSeenTime);
                                }
                                if(3000>de&&de>2000){
                                    System.out.println("超时小于3s:"+carNum);
                                    System.out.println(lastSeenTime);
                                }
                                if(4000>de&&de>3000){
                                    System.out.println("超时小于4s:"+carNum);
                                    System.out.println(lastSeenTime);
                                }
                                if(5000>de&&de>4000){
                                    System.out.println("超时小于5s:"+carNum);
                                    System.out.println(lastSeenTime);
                                }

                                // 判断是否超时
                                if (de > SESSION_TIMEOUT_MS) {
                                    timeoutCars.add(carNum);
                                }
                            }

                            // 处理超时车辆
                            if (!timeoutCars.isEmpty()) {
                                for (String carNum : timeoutCars) {
                                    out.collect(new Tuple4<>(
                                            mapTimeSeg.get(carNum),
                                            mapType.get(carNum),
                                            lastSeenTime.get(carNum),
                                            map.get(carNum)
                                    ));

                                    // 清除所有状态
                                    map.remove(carNum);
                                    mapTimeSeg.remove(carNum);
                                    mapType.remove(carNum);
                                    lastSeenTime.remove(carNum);
                                    lastSampleTime.remove(carNum); // 清理采样时间记录

                                }
                            }
                        } catch (Exception e) {
                            System.err.println("解析 JSON 时出错: " + e.getMessage());
                            e.printStackTrace();
                        }
                    }
                })
                .addSink(new DynamicHBaseSink("ZCarTraj", "cf0"));

        env.execute("Trajectory to HBase");
    }

    private static class DynamicHBaseSink extends RichSinkFunction<Tuple4<String, Integer, Long, List<Tuple5<Double, Double, Integer, Integer, Double>>>> {
        private final String baseTableName;
        private final String columnFamily;

        private transient Connection connection;
        private transient Table table;

        private transient String currentTableName;
        private transient LocalDateTime nextTableSwitchTime;
        private final ReentrantLock tableLock = new ReentrantLock();
        private static final ConcurrentHashMap<String, Object> tableCreationLocks = new ConcurrentHashMap<>();

        public DynamicHBaseSink(String baseTableName, String columnFamily) {
            this.baseTableName = baseTableName;
            this.columnFamily = columnFamily;
        }

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            super.open(parameters);
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142,10.48.53.80");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            conf.set("zookeeper.session.timeout", "120000");
            conf.set("fs.defaultFS", "hdfs://100.65.38.139:9000");
            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            connection = ConnectionFactory.createConnection(conf);
            currentTableName = null;
            nextTableSwitchTime = null;
        }

        @Override
        public void invoke(Tuple4<String, Integer, Long, List<Tuple5<Double, Double, Integer, Integer, Double>>> value, Context context) throws Exception {
            tableLock.lock();
            try {
                String rowKey = value.f0;

//                long rowKeyTime = value.f2;
                long rowKeyTime = Long.parseLong(value.f0.split("-")[0]);
                // 切换表（如果必要）
                if (currentTableName == null || isTimeToSwitch(rowKeyTime)) {
                    switchTable(rowKeyTime);
                }
                // 插入数据
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

                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
                currentTableName = baseTableName + "_" + rowKeyDateTime.format(formatter);
                nextTableSwitchTime = rowKeyDateTime.toLocalDate().atStartOfDay().plusDays(1);

                createTableIfNotExists(currentTableName, columnFamily);
                if (table != null) table.close();
                table = connection.getTable(TableName.valueOf(currentTableName));

                System.out.printf("切换到新表: %s，下一次切换时间: %s%n",
                        currentTableName, nextTableSwitchTime.format(DateTimeFormatter.ISO_LOCAL_DATE));
            } finally {
                tableLock.unlock();
            }
        }

        public void createTableIfNotExists(String tableName, String columnFamily) throws IOException {
            tableLock.lock();
            try (Admin admin = connection.getAdmin()) {
                TableName hbaseTableName = TableName.valueOf(tableName);
                Object lock = tableCreationLocks.computeIfAbsent(tableName, k -> new Object());

                synchronized (lock) {
                    if (!admin.tableExists(hbaseTableName)) {
                        HTableDescriptor tableDescriptor = new HTableDescriptor(hbaseTableName);
                        tableDescriptor.addFamily(new HColumnDescriptor(columnFamily));
                        admin.createTable(tableDescriptor);
                        System.out.println("创建新表: " + tableName);
                    } else {
                        System.out.println("表已存在: " + tableName);
                    }
                }
            } finally {
                tableLock.unlock();
            }
        }
    }
}