package whu.edu.moniData.ingest.holyAnalysisJob.useFul;


import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import redis.clients.jedis.*;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class CombinedCarTrajAnalysisJob_RedisAndHBase {

    // Redis 配置
    private static final String REDIS_HOST = "100.65.38.141";
    private static final int REDIS_PORT = 6380;
    private static final String REDIS_PASSWORD = "whdx123cgz666";

    // Redis键前缀
    private static final String VEHICLE_PREFIX = "vehicle:";
    private static final String METADATA_SUFFIX = ":metadata";
    private static final String TRAJECTORY_SUFFIX = ":trajectory";
    private static final String LAST_SEEN_SUFFIX = ":last_seen";
    private static final String LAST_SAMPLE_SUFFIX = ":last_sample";

    private static JedisPool jedisPool;

    public static void main(String[] args) throws Exception {
        // 初始化Redis连接池
        initRedisPool();

        // 程序启动时清空Redis数据
        cleanRedisOnStartup();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4); // 设置合理的并行度

        // 添加关闭钩子，程序结束时清空Redis数据
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            cleanRedisOnShutdown();
        }));

        // ================== Kafka 配置 ==================
        String primaryBrokers = "10.48.53.82:9092";
        String groupId = "flink-combined-group";

        // ================== 主数据源 ==================
        List<String> primaryTopics = Arrays.asList("MergedPathData.sceneTest.1",
                "MergedPathData.sceneTest.2",
                "MergedPathData.sceneTest.3",
                "MergedPathData.sceneTest.4",
                "MergedPathData.sceneTest.5",
                "MergedPathData.sceneTest.6",
                "MergedPathData.sceneTest.7",
                "MergedPathData.sceneTest.8",
                "MergedPathData.sceneTest.9",
                "MergedPathData.sceneTest.10",
                "MergedPathData.sceneTest.11");

        KafkaSource<String> primarySource = KafkaSource.<String>builder()
                .setBootstrapServers(primaryBrokers)
                .setTopics(primaryTopics)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setProperty("auto.offset.commit", "true")
                .setProperty("consumer.max.poll.interval.ms", String.valueOf(24 * 60 * 60 * 1000))
                .setProperty("session.timeout.ms", String.valueOf(24 * 60 * 60 * 1000))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> primaryStream = env.fromSource(
                primarySource, WatermarkStrategy.noWatermarks(), "Primary Kafka Source");

        // ================== 处理主数据流 ==================
        SingleOutputStreamOperator<Tuple6<String, Integer, Long, List<Tuple4<Double, Double, Integer, Double>>, Integer, String>> primaryProcessed =
                primaryStream.flatMap(new PrimaryTrajectoryProcessor())
                        .name("Primary Trajectory Processor");

        // ================== 输出到HBase ==================
        primaryProcessed.addSink(new PrimaryHBaseSink("ZCarTraj", "cf0"))
                .name("Primary HBase Sink")
                .setParallelism(2);

        env.execute("Trajectory Analysis Job with Redis Storage and HBase Output");
    }

    private static void initRedisPool() {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(200);
        poolConfig.setMaxIdle(32);
        poolConfig.setMinIdle(10);
        poolConfig.setMaxWaitMillis(100 * 1000);
        poolConfig.setBlockWhenExhausted(true);
        poolConfig.setTestOnBorrow(true);

        jedisPool = new JedisPool(poolConfig, REDIS_HOST, REDIS_PORT, 60000, REDIS_PASSWORD);
        System.out.println("Redis连接池初始化成功");
    }

    // 程序启动时清空Redis数据
    private static void cleanRedisOnStartup() {
        try (Jedis jedis = jedisPool.getResource()) {
            System.out.println("程序启动: 清空Redis数据...");
            cleanRedis(jedis);
            System.out.println("Redis数据已清空");
        } catch (Exception e) {
            System.err.println("清空Redis数据失败: " + e.getMessage());
        }
    }

    // 程序关闭时清空Redis数据
    private static void cleanRedisOnShutdown() {
        try (Jedis jedis = jedisPool.getResource()) {
            System.out.println("程序关闭: 清空Redis数据...");
            cleanRedis(jedis);
            System.out.println("Redis数据已清空");
        } catch (Exception e) {
            System.err.println("清空Redis数据失败: " + e.getMessage());
        } finally {
            if (jedisPool != null) {
                jedisPool.close();
                System.out.println("Redis连接池已关闭");
            }
        }
    }

    // 清空Redis数据
    private static void cleanRedis(Jedis jedis) {
        String cursor = "0";
        ScanParams scanParams = new ScanParams().match(VEHICLE_PREFIX + "*").count(100);
        int deletedCount = 0;

        do {
            ScanResult<String> scanResult = jedis.scan(cursor, scanParams);
            cursor = scanResult.getCursor();
            List<String> keys = scanResult.getResult();

            if (!keys.isEmpty()) {
                jedis.del(keys.toArray(new String[0]));
                deletedCount += keys.size();
                System.out.println("删除 " + keys.size() + " 个键");
            }
        } while (!cursor.equals("0"));

        System.out.println("总共删除 " + deletedCount + " 个键");
    }

    // ================== 主数据处理逻辑 ==================
    private static class PrimaryTrajectoryProcessor implements FlatMapFunction<String,
            Tuple6<String, Integer, Long, List<Tuple4<Double, Double, Integer, Double>>, Integer, String>> {

        private static final long SESSION_TIMEOUT_MS = 10000;
        private static final long SAMPLING_INTERVAL_MS = 1000;

        private final ReentrantLock stateLock = new ReentrantLock();

        @Override
        public void flatMap(String jsonString,
                            Collector<Tuple6<String, Integer, Long, List<Tuple4<Double, Double, Integer, Double>>, Integer, String>> out) {

            stateLock.lock();
            Jedis jedis = null;
            try {
                jedis = jedisPool.getResource();

                JSONObject jsonObject = new JSONObject(jsonString);
                long timeObs = parseTimestamp(jsonObject.getString("timeStamp"));
                JSONArray tdataArray = jsonObject.getJSONArray("pathList");

                for (int i = 0; i < tdataArray.length(); i++) {
                    JSONObject tdataObject = tdataArray.getJSONObject(i);
                    String plateNo = tdataObject.getString("plateNo");
                    if(plateNo == null) continue;

                    String id = String.valueOf(tdataObject.getLong("id"));

                    // 创建车辆唯一标识
                    String vehicleKey = VEHICLE_PREFIX + plateNo + ":" + id;

                    // 更新最后看到时间
                    jedis.set(vehicleKey + LAST_SEEN_SUFFIX, String.valueOf(timeObs));
                    jedis.expire(vehicleKey + LAST_SEEN_SUFFIX, 24 * 60 * 60);

                    // 检查最后采样时间
                    String lastSampleStr = jedis.get(vehicleKey + LAST_SAMPLE_SUFFIX);
                    long lastSample = (lastSampleStr != null) ? Long.parseLong(lastSampleStr) : 0L;

                    if (timeObs - lastSample >= SAMPLING_INTERVAL_MS) {
                        // 检查是否是新车辆
                        if (!jedis.exists(vehicleKey + METADATA_SUFFIX)) {
                            initializeNewVehicle(jedis, vehicleKey, plateNo, tdataObject, timeObs);
                        } else {
                            updateVehicleTrajectory(jedis, vehicleKey, tdataObject, timeObs);
                        }

                        // 更新最后采样时间
                        jedis.set(vehicleKey + LAST_SAMPLE_SUFFIX, String.valueOf(timeObs));
                        jedis.expire(vehicleKey + LAST_SAMPLE_SUFFIX, 24 * 60 * 60);
                    }
                }

                processTimeoutVehicles(jedis, timeObs, out);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (jedis != null) {
                    jedis.close();
                }
                stateLock.unlock();
            }
        }

        private long parseTimestamp(String timestampStr) throws Exception {
            try {
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS");
                LocalDateTime localDateTime = LocalDateTime.parse(timestampStr, formatter);
                return localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
            } catch (Exception e) {
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SS");
                LocalDateTime localDateTime = LocalDateTime.parse(timestampStr, formatter);
                return localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
            }
        }

        private void initializeNewVehicle(Jedis jedis, String vehicleKey, String plateNo, JSONObject tdata, long timestamp) {
            // 存储元数据
            Map<String, String> metadata = new HashMap<>();
            metadata.put("plateNo", plateNo);
            metadata.put("vehicleType", String.valueOf(tdata.getInt("vehicleType")));
            metadata.put("timeSeg", timestamp + "-" + plateNo + "-" + vehicleKey.split(":")[2]);
            jedis.hmset(vehicleKey + METADATA_SUFFIX, metadata);
            jedis.expire(vehicleKey + METADATA_SUFFIX, 24 * 60 * 60);

            // 初始化轨迹列表
            String trajectoryKey = vehicleKey + TRAJECTORY_SUFFIX;
            JSONArray trajectoryArray = new JSONArray();
            JSONObject pointJson = new JSONObject();
            pointJson.put("longitude", tdata.getDouble("longitude"));
            pointJson.put("latitude", tdata.getDouble("latitude"));
            pointJson.put("laneNo", tdata.getInt("laneNo"));
            pointJson.put("direction", getDirectionSafely(tdata));
            pointJson.put("speed", tdata.getDouble("speed"));
            trajectoryArray.put(pointJson);
            jedis.set(trajectoryKey, trajectoryArray.toString());
            jedis.expire(trajectoryKey, 24 * 60 * 60);
        }

        private void updateVehicleTrajectory(Jedis jedis, String vehicleKey, JSONObject tdata, long timestamp) {
            // 更新最后看到时间
            jedis.set(vehicleKey + LAST_SEEN_SUFFIX, String.valueOf(timestamp));
            jedis.expire(vehicleKey + LAST_SEEN_SUFFIX, 24 * 60 * 60);

            // 更新轨迹
            String trajectoryKey = vehicleKey + TRAJECTORY_SUFFIX;
            String trajectoryJson = jedis.get(trajectoryKey);

            JSONArray trajectoryArray;
            if (trajectoryJson == null) {
                // 如果轨迹不存在，创建新的轨迹数组
                trajectoryArray = new JSONArray();
                System.out.println("警告: 轨迹键不存在，创建新轨迹数组: " + trajectoryKey);
            } else {
                trajectoryArray = new JSONArray(trajectoryJson);
            }

            JSONObject pointJson = new JSONObject();
            pointJson.put("longitude", tdata.getDouble("longitude"));
            pointJson.put("latitude", tdata.getDouble("latitude"));
            pointJson.put("laneNo", tdata.getInt("laneNo"));
            pointJson.put("direction", getDirectionSafely(tdata));
            pointJson.put("speed", tdata.getDouble("speed"));
            trajectoryArray.put(pointJson);

            jedis.set(trajectoryKey, trajectoryArray.toString());
            jedis.expire(trajectoryKey, 24 * 60 * 60);
        }

        private void processTimeoutVehicles(Jedis jedis, long currentTime,
                                            Collector<Tuple6<String, Integer, Long, List<Tuple4<Double, Double, Integer, Double>>, Integer, String>> out) {

            // 扫描所有车辆键
            Set<String> vehicleKeys = new HashSet<>();
            String cursor = "0";
            ScanParams scanParams = new ScanParams().match(VEHICLE_PREFIX + "*" + LAST_SEEN_SUFFIX).count(100);
            do {
                ScanResult<String> scanResult = jedis.scan(cursor, scanParams);
                cursor = scanResult.getCursor();
                for (String key : scanResult.getResult()) {
                    // 提取车辆标识 (去掉后缀)
                    String vehicleKey = key.substring(0, key.length() - LAST_SEEN_SUFFIX.length());
                    vehicleKeys.add(vehicleKey);
                }
            } while (!cursor.equals("0"));

            // 检查超时车辆
            Set<String> timeoutKeys = new HashSet<>();
            for (String vehicleKey : vehicleKeys) {
                String lastSeenStr = jedis.get(vehicleKey + LAST_SEEN_SUFFIX);
                if (lastSeenStr != null) {
                    long lastSeenTime = Long.parseLong(lastSeenStr);
                    if (currentTime - lastSeenTime > SESSION_TIMEOUT_MS) {
                        timeoutKeys.add(vehicleKey);
                    }
                }
            }

            // 处理超时车辆
            for (String vehicleKey : timeoutKeys) {
                // 从Redis获取元数据
                Map<String, String> metadata = jedis.hgetAll(vehicleKey + METADATA_SUFFIX);
                if (metadata == null || metadata.isEmpty()) {
                    continue;
                }

                // 构建轨迹数据
                String timeSeg = metadata.get("timeSeg");
                int type = Integer.parseInt(metadata.get("vehicleType"));
                long latestTime = Long.parseLong(jedis.get(vehicleKey + LAST_SEEN_SUFFIX));

                // 从Redis获取轨迹数据
                String trajectoryKey = vehicleKey + TRAJECTORY_SUFFIX;
                String trajectoryJsonStr = jedis.get(trajectoryKey);
                if (trajectoryJsonStr == null) {
                    continue;
                }

                JSONArray trajectoryArray = new JSONArray(trajectoryJsonStr);
                int dir = trajectoryArray.getJSONObject(0).getInt("direction");
                List<Tuple4<Double, Double, Integer, Double>> trajectory = new ArrayList<>();

                for (int i = 0; i < trajectoryArray.length(); i++) {
                    JSONObject point = trajectoryArray.getJSONObject(i);
                    trajectory.add(new Tuple4<>(
                            point.getDouble("longitude"),
                            point.getDouble("latitude"),
                            point.getInt("laneNo"),
                            point.getDouble("speed")
                    ));
                }

                String eventList = "[]"; // 默认空事件列表

                // 输出到HBase Sink
                out.collect(new Tuple6<>(timeSeg, type, latestTime, trajectory, dir, eventList));

                // 清理Redis数据
                cleanupVehicle(jedis, vehicleKey);
            }
        }

        private void cleanupVehicle(Jedis jedis, String vehicleKey) {
            // 删除所有相关Redis键
            jedis.del(
                    vehicleKey + METADATA_SUFFIX,
                    vehicleKey + TRAJECTORY_SUFFIX,
                    vehicleKey + LAST_SEEN_SUFFIX,
                    vehicleKey + LAST_SAMPLE_SUFFIX
            );
        }

        // 安全获取方法
        private int getDirectionSafely(JSONObject tdata) {
            try { return tdata.getInt("direction"); }
            catch (JSONException e) { return -1; }
        }
    }

    // ================== 主数据HBase Sink ==================
    private static class PrimaryHBaseSink extends RichSinkFunction<Tuple6<String, Integer, Long,
            List<Tuple4<Double, Double, Integer, Double>>, Integer, String>> {

        private final String baseTableName;
        private final String columnFamily;
        private transient Connection connection;
        private transient Table currentTable;
        private transient String currentTableName;
        private final ReentrantLock tableLock = new ReentrantLock();
        private static final ConcurrentHashMap<String, Object> tableCreationLocks = new ConcurrentHashMap<>();

        public PrimaryHBaseSink(String baseTableName, String columnFamily) {
            this.baseTableName = baseTableName;
            this.columnFamily = columnFamily;
        }

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            super.open(parameters);
            Configuration conf = createHBaseConfig();
            connection = ConnectionFactory.createConnection(conf);
        }

        @Override
        public void invoke(Tuple6<String, Integer, Long, List<Tuple4<Double, Double, Integer, Double>>,
                Integer, String> value, Context context) throws Exception {

            tableLock.lock();
            try {
                String rowKey = value.f0;
                long rowKeyTime = parseRowKeyTime(rowKey);

                switchTableIfNeeded(rowKeyTime);

                Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("type"), Bytes.toBytes(value.f1.toString()));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("event_list"), Bytes.toBytes(value.f5));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("latest_time"), Bytes.toBytes(value.f2.toString()));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("trajectory"), Bytes.toBytes(value.f3.toString()));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("direction"), Bytes.toBytes(value.f4.toString()));

                currentTable.put(put);
            } catch (Exception e) {
                System.err.println("主数据HBase写入失败: " + e.getMessage());
                resetConnection();
            } finally {
                tableLock.unlock();
            }
        }

        private long parseRowKeyTime(String rowKey) {
            try {
                return Long.parseLong(rowKey.split("-")[0]);
            } catch (NumberFormatException e) {
                System.err.println("无效的主数据rowKey格式: " + rowKey);
                return System.currentTimeMillis();
            }
        }

        private void switchTableIfNeeded(long rowKeyTime) throws IOException {
            LocalDateTime rowKeyDateTime = LocalDateTime.ofInstant(
                    Instant.ofEpochMilli(rowKeyTime), ZoneId.systemDefault()
            );
            String newTableName = baseTableName + "_" + rowKeyDateTime.format(DateTimeFormatter.BASIC_ISO_DATE);

            if (currentTable == null || !newTableName.equals(currentTableName)) {
                tableLock.lock();
                try {
                    if (currentTable == null || !newTableName.equals(currentTableName)) {
                        createTableIfNotExists(newTableName);
                        if (currentTable != null) currentTable.close();
                        currentTable = connection.getTable(TableName.valueOf(newTableName));
                        currentTableName = newTableName;
                        System.out.println("主数据切换到HBase表: " + currentTableName);
                    }
                } finally {
                    tableLock.unlock();
                }
            }
        }

        private void createTableIfNotExists(String tableName) throws IOException {
            Object lock = tableCreationLocks.computeIfAbsent(tableName, k -> new Object());
            synchronized (lock) {
                try (Admin admin = connection.getAdmin()) {
                    TableName tn = TableName.valueOf(tableName);
                    if (!admin.tableExists(tn)) {
                        HTableDescriptor tableDescriptor = new HTableDescriptor(tn);
                        HColumnDescriptor cfDesc = new HColumnDescriptor(columnFamily);
                        tableDescriptor.addFamily(cfDesc);
                        admin.createTable(tableDescriptor);
                        System.out.println("创建主数据HBase表: " + tableName);
                    }
                }
            }
        }

        private void resetConnection() {
            try {
                if (connection != null) connection.close();
                Configuration conf = createHBaseConfig();
                connection = ConnectionFactory.createConnection(conf);
                if (currentTableName != null) {
                    currentTable = connection.getTable(TableName.valueOf(currentTableName));
                }
            } catch (IOException ex) {
                System.err.println("重建HBase连接失败: " + ex.getMessage());
            }
        }

        @Override
        public void close() throws Exception {
            try {
                if (currentTable != null) currentTable.close();
            } finally {
                if (connection != null) connection.close();
            }
        }
    }

    // ================== 公共配置方法 ==================
    private static Configuration createHBaseConfig() {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142,10.48.53.80");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("zookeeper.session.timeout", "120000");
        conf.set("hbase.rpc.timeout", "300000");
        conf.set("fs.defaultFS", "hdfs://100.65.38.139:9000");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        return conf;
    }
}
