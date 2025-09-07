package whu.edu.moniData.ingest.holyAnalysisJob;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantLock;

public class CombinedCarTrajAnalysisJob_Redis_HBase {

    // Redis 配置
    private static final String REDIS_HOST = "100.65.38.141";
    private static final int REDIS_PORT = 6380;
    private static final String REDIS_PASSWORD = "whdx123cgz666";

    // Redis键前缀
    private static final String REDIS_METADATA_PREFIX = "v:mata:";
    private static final String REDIS_TRAJECTORY_PREFIX = "v:trj:";
    private static final String REDIS_LAST_SEEN_PREFIX = "v:last_seen:";
    private static final String REDIS_LAST_SAMPLE_PREFIX = "v:last_sample:";

    // HBase 配置
    private static final String HBASE_BASE_TABLE_NAME = "Z822CarTraj";
    private static final String HBASE_COLUMN_FAMILY = "cf0";

    private static JedisPool jedisPool;
    private static Connection hbaseConnection;
    private static Table currentTable;
    private static String currentTableName;
    private static LocalDateTime nextTableSwitchTime;
    private static final ReentrantLock tableLock = new ReentrantLock();
    private static final ConcurrentHashMap<String, Object> tableCreationLocks = new ConcurrentHashMap<>();

    // 批量导入相关配置
    private static final int BATCH_SIZE = 1000; // 每批处理的数据量
    private static final long BATCH_INTERVAL_MS = 5000; // 批量处理的时间间隔(ms)
    private static List<JSONObject> batchBuffer = new CopyOnWriteArrayList<>(); // 使用线程安全列表
    private static long lastBatchProcessTime = 0;

    // 程序运行状态标志
    private static volatile boolean isRunning = true;

    public static void main(String[] args) throws Exception {
        // 初始化Redis连接池
        initRedisPool();

        // 初始化HBase连接
        initHBaseConnection();

        // 程序启动时清空Redis数据
        cleanRedisOnStartup();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 添加关闭钩子，程序结束时清空Redis数据和刷新HBase批量数据
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            isRunning = false; // 设置运行状态为false
            try {
                Thread.sleep(2000); // 等待2秒让处理中的任务完成
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            flushHBaseBatch(); // 确保所有数据都已写入HBase
            closeHBaseConnection();
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

        // ================== 创建输出Sink ==================
        // 主输出 (trajectoryoutput) - 保留用于其他非超时数据
        KafkaSink<String> primarySink = KafkaSink.<String>builder()
                .setBootstrapServers(primaryBrokers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("trajectoryoutput")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();

        // ================== 处理主数据流 ==================
        SingleOutputStreamOperator<String> primaryProcessed = primaryStream
                .flatMap(new PrimaryTrajectoryProcessor())
                .name("Primary Trajectory Processor");

        // ================== 输出结果 ==================
        primaryProcessed.sinkTo(primarySink).name("Primary Output Sink");

        env.execute("Trajectory Analysis Job with Redis and HBase Batch Import");
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

    private static void initHBaseConnection() {
        try {
            Configuration config = HBaseConfiguration.create();
            config.set("hbase.zookeeper.quorum", "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142,10.48.53.80");
            config.set("hbase.zookeeper.property.clientPort", "2181");
            config.set("zookeeper.session.timeout", "120000");
            config.set("fs.defaultFS", "hdfs://100.65.38.139:9000");
            config.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            hbaseConnection = ConnectionFactory.createConnection(config);
            System.out.println("HBase连接初始化成功");
        } catch (Exception e) {
            System.err.println("HBase连接初始化失败: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void closeHBaseConnection() {
        try {
            if (currentTable != null) {
                currentTable.close();
            }
            if (hbaseConnection != null) {
                hbaseConnection.close();
            }
            System.out.println("HBase连接已关闭");
        } catch (Exception e) {
            System.err.println("关闭HBase连接失败: " +e.getMessage());
        }
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
            if (jedisPool != null && !jedisPool.isClosed()) {
                jedisPool.close();
                System.out.println("Redis连接池已关闭");
            }
        }
    }

    // 清空Redis数据
    private static void cleanRedis(Jedis jedis) {
        // 删除所有元数据键
        deleteKeysByPattern(jedis, REDIS_METADATA_PREFIX + "*");
        // 删除所有轨迹键
        deleteKeysByPattern(jedis, REDIS_TRAJECTORY_PREFIX + "*");
        // 删除所有最后看到时间键
        deleteKeysByPattern(jedis, REDIS_LAST_SEEN_PREFIX + "*");
        // 删除所有最后采样时间键
        deleteKeysByPattern(jedis, REDIS_LAST_SAMPLE_PREFIX + "*");
    }

    // 按模式删除键
    private static void deleteKeysByPattern(Jedis jedis, String pattern) {
        String cursor = "0";
        ScanParams scanParams = new ScanParams().match(pattern).count(100);
        int deletedCount = 0;

        do {
            ScanResult<String> scanResult = jedis.scan(cursor, scanParams);
            cursor = scanResult.getCursor();
            List<String> keys = scanResult.getResult();

            if (!keys.isEmpty()) {
                jedis.del(keys.toArray(new String[0]));
                deletedCount += keys.size();
                System.out.println("删除 " + keys.size() + " 个键 (模式: " + pattern + ")");
            }
        } while (!cursor.equals("0"));

        System.out.println("总共删除 " + deletedCount + " 个键 (模式: " + pattern + ")");
    }

    // 批量导入数据到HBase
    private static void batchImportToHBase(JSONObject vehicleData) {
        if (!isRunning) {
            return; // 如果程序正在关闭，不再添加新数据
        }

        batchBuffer.add(vehicleData);

        // 检查是否达到批量处理条件
        long currentTime = System.currentTimeMillis();
        if (batchBuffer.size() >= BATCH_SIZE ||
                (currentTime - lastBatchProcessTime) >= BATCH_INTERVAL_MS) {
            flushHBaseBatch();
            lastBatchProcessTime = currentTime;
        }
    }

    // 将批量缓冲区中的数据写入HBase
    private static void flushHBaseBatch() {
        if (batchBuffer.isEmpty()) {
            return;
        }

        // 创建数据副本进行处理，避免在迭代过程中修改原始列表
        List<JSONObject> batchCopy = new ArrayList<>(batchBuffer);
        batchBuffer.clear(); // 清空原始列表

        try {
            List<Put> puts = new ArrayList<>();

            for (JSONObject vehicleData : batchCopy) {
                try {
                    String timeSeg = vehicleData.getString("timeSeg");
                    String rowKey = timeSeg; // 使用timeSeg作为行键

                    // 切换表（如果必要）
                    long rowKeyTime = Long.parseLong(timeSeg.split("-")[0]);
                    if (currentTableName == null || isTimeToSwitch(rowKeyTime)) {
                        switchTable(rowKeyTime);
                    }

                    Put put = new Put(Bytes.toBytes(rowKey));

                    // 添加基本数据
                    put.addColumn(Bytes.toBytes(HBASE_COLUMN_FAMILY),
                            Bytes.toBytes("type"),
                            Bytes.toBytes(String.valueOf(vehicleData.getInt("type"))));

                    put.addColumn(Bytes.toBytes(HBASE_COLUMN_FAMILY),
                            Bytes.toBytes("latestTime"),
                            Bytes.toBytes(String.valueOf(vehicleData.getLong("latestTime"))));

                    // 安全地添加轨迹数据
                    if (vehicleData.has("trajectory")) {
                        JSONArray trajectory = vehicleData.getJSONArray("trajectory");
                        put.addColumn(Bytes.toBytes(HBASE_COLUMN_FAMILY),
                                Bytes.toBytes("trajectory"),
                                Bytes.toBytes(trajectory.toString()));
                    } else {
                        // 如果轨迹数据不存在，添加空数组
                        put.addColumn(Bytes.toBytes(HBASE_COLUMN_FAMILY),
                                Bytes.toBytes("trajectory"),
                                Bytes.toBytes(new JSONArray().toString()));
                    }

                    // 安全地添加事件列表
                    if (vehicleData.has("eventList")) {
                        JSONArray eventList = vehicleData.getJSONArray("eventList");
                        put.addColumn(Bytes.toBytes(HBASE_COLUMN_FAMILY),
                                Bytes.toBytes("eventList"),
                                Bytes.toBytes(eventList.toString()));
                    } else {
                        // 如果事件列表不存在，添加空数组
                        put.addColumn(Bytes.toBytes(HBASE_COLUMN_FAMILY),
                                Bytes.toBytes("eventList"),
                                Bytes.toBytes(new JSONArray().toString()));
                    }

                    puts.add(put);
                } catch (Exception e) {
                    System.err.println("处理车辆数据失败: " + e.getMessage());
                    e.printStackTrace();
                    // 将处理失败的数据重新添加回缓冲区
                    batchBuffer.add(vehicleData);
                }
            }

            // 批量写入HBase
            if (!puts.isEmpty()) {
                currentTable.put(puts);
                System.out.println("成功批量导入 " + puts.size() + " 条车辆轨迹数据到HBase表: " + currentTableName);
            }

        } catch (Exception e) {
            System.err.println("批量导入HBase失败: " + e.getMessage());
            e.printStackTrace();
            // 如果发生异常，将数据重新添加回缓冲区
            batchBuffer.addAll(batchCopy);
        }
    }

    // 检查是否需要切换表
    private static boolean isTimeToSwitch(long rowKeyTime) {
        LocalDateTime rowKeyDateTime = LocalDateTime.ofInstant(
                Instant.ofEpochMilli(rowKeyTime), ZoneId.systemDefault()
        );
        return rowKeyDateTime.isAfter(nextTableSwitchTime);
    }

    // 切换到新表
    private static void switchTable(long rowKeyTime) throws IOException {
        tableLock.lock();
        try {
            LocalDateTime rowKeyDateTime = LocalDateTime.ofInstant(
                    Instant.ofEpochMilli(rowKeyTime), ZoneId.systemDefault()
            );

            // 创建动态表名（按天分表）
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
            currentTableName = HBASE_BASE_TABLE_NAME + "_" + rowKeyDateTime.format(formatter);
            nextTableSwitchTime = rowKeyDateTime.toLocalDate().atStartOfDay().plusDays(1);

            // 创建表（如果不存在）
            createTableIfNotExists(currentTableName, HBASE_COLUMN_FAMILY);

            // 关闭旧表（如果存在）
            if (currentTable != null) {
                currentTable.close();
            }

            // 获取新表
            currentTable = hbaseConnection.getTable(TableName.valueOf(currentTableName));

            System.out.printf("切换到新表: %s，下一次切换时间: %s%n",
                    currentTableName, nextTableSwitchTime.format(DateTimeFormatter.ISO_LOCAL_DATE));
        } finally {
            tableLock.unlock();
        }
    }

    // 创建HBase表（如果不存在）
    private static void createTableIfNotExists(String tableName, String columnFamily) throws IOException {
        tableLock.lock();
        try (Admin admin = hbaseConnection.getAdmin()) {
            TableName hbaseTableName = TableName.valueOf(tableName);
            Object lock = tableCreationLocks.computeIfAbsent(tableName, k -> new Object());

            synchronized (lock) {
                if (!admin.tableExists(hbaseTableName)) {
                    HTableDescriptor tableDescriptor = new HTableDescriptor(hbaseTableName);
                    tableDescriptor.addFamily(new HColumnDescriptor(columnFamily));
                    admin.createTable(tableDescriptor);
                    System.out.println("创建HBase表: " + tableName);
                } else {
                    System.out.println("HBase表已存在: " + tableName);
                }
            }
        } finally {
            tableLock.unlock();
        }
    }

    // ================== 主数据处理逻辑 ==================
    private static class PrimaryTrajectoryProcessor implements FlatMapFunction<String, String> {
        private static final long SESSION_TIMEOUT_MS = 10000;
        private static final long SAMPLING_INTERVAL_MS = 1000;

        private final ReentrantLock stateLock = new ReentrantLock();

        @Override
        public void flatMap(String jsonString, Collector<String> out) {
            if (!isRunning) {
                return; // 如果程序正在关闭，不再处理新数据
            }

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
                    String id = String.valueOf(tdataObject.getLong("id"));

                    // 更新最后看到时间
                    String lastSeenKey = REDIS_LAST_SEEN_PREFIX + id;
                    jedis.set(lastSeenKey, String.valueOf(timeObs));
                    jedis.expire(lastSeenKey, 24 * 60 * 60);

                    // 检查最后采样时间
                    String lastSampleKey = REDIS_LAST_SAMPLE_PREFIX + id;
                    String lastSampleStr = jedis.get(lastSampleKey);
                    long lastSample = (lastSampleStr != null) ? Long.parseLong(lastSampleStr) : 0L;

                    if (timeObs - lastSample >= SAMPLING_INTERVAL_MS) {
                        // 检查是否是新车辆
                        String metadataKey = REDIS_METADATA_PREFIX + id;
                        if (!jedis.exists(metadataKey)) {
                            initializeNewVehicle(jedis, id, plateNo, tdataObject, timeObs);
                        } else {
                            updateVehicleTrajectory(jedis, id, tdataObject, timeObs);
                        }

                        // 更新最后采样时间
                        jedis.set(lastSampleKey, String.valueOf(timeObs));
                        jedis.expire(lastSampleKey, 24 * 60 * 60);
                    }
                }

                processTimeoutVehicles(jedis, timeObs);
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

        private void initializeNewVehicle(Jedis jedis, String id, String plateNo, JSONObject tdata, long timestamp) {
            // 存储元数据到Redis
            String metadataKey = REDIS_METADATA_PREFIX + id;
            Map<String, String> metadata = new HashMap<>();
            metadata.put("plateNo", plateNo);
            metadata.put("vehicleType", String.valueOf(tdata.getInt("vehicleType")));
            metadata.put("timeSeg", timestamp + "-" + plateNo + "-" + id);
            jedis.hmset(metadataKey, metadata);
            jedis.expire(metadataKey, 24 * 60 * 60);

            // 初始化轨迹列表到Redis
            String trajectoryKey = REDIS_TRAJECTORY_PREFIX + id;
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

        private void updateVehicleTrajectory(Jedis jedis, String id, JSONObject tdata, long timestamp) {
            // 更新最后看到时间
            String lastSeenKey = REDIS_LAST_SEEN_PREFIX + id;
            jedis.set(lastSeenKey, String.valueOf(timestamp));
            jedis.expire(lastSeenKey, 24 * 60 * 60);

            // 更新轨迹
            String trajectoryKey = REDIS_TRAJECTORY_PREFIX + id;
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

        private void processTimeoutVehicles(Jedis jedis, long currentTime) {
            // 扫描所有车辆ID
            Set<String> vehicleIds = new HashSet<>();
            String cursor = "0";
            ScanParams scanParams = new ScanParams().match(REDIS_METADATA_PREFIX + "*").count(100);
            do {
                ScanResult<String> scanResult = jedis.scan(cursor, scanParams);
                cursor = scanResult.getCursor();
                for (String key : scanResult.getResult()) {
                    String id = key.substring(REDIS_METADATA_PREFIX.length());
                    vehicleIds.add(id);
                }
            } while (!cursor.equals("0"));

            // 检查超时车辆
            Set<String> timeoutIds = new HashSet<>();
            for (String id : vehicleIds) {
                String lastSeenKey = REDIS_LAST_SEEN_PREFIX + id;
                String lastSeenStr = jedis.get(lastSeenKey);
                if (lastSeenStr != null) {
                    long lastSeenTime = Long.parseLong(lastSeenStr);
                    if (currentTime - lastSeenTime > SESSION_TIMEOUT_MS) {
                        timeoutIds.add(id);
                    }
                }
            }

            // 处理超时车辆
            for (String id : timeoutIds) {
                // 从Redis获取元数据
                String metadataKey = REDIS_METADATA_PREFIX + id;
                Map<String, String> metadata = jedis.hgetAll(metadataKey);
                if (metadata == null || metadata.isEmpty()) {
                    continue;
                }

                // 构建轨迹JSON
                JSONObject trajectoryJson = new JSONObject();
                trajectoryJson.put("timeSeg", metadata.get("timeSeg"));
                trajectoryJson.put("type", Integer.parseInt(metadata.get("vehicleType")));

                // 安全获取latestTime
                String lastSeenKey = REDIS_LAST_SEEN_PREFIX + id;
                String lastSeenStr = jedis.get(lastSeenKey);
                if (lastSeenStr != null) {
                    trajectoryJson.put("latestTime", Long.parseLong(lastSeenStr));
                } else {
                    trajectoryJson.put("latestTime", currentTime);
                }

                trajectoryJson.put("eventList", new JSONArray());

                // 从Redis获取轨迹数据
                String trajectoryKey = REDIS_TRAJECTORY_PREFIX + id;
                String trajectoryJsonStr = jedis.get(trajectoryKey);
                if (trajectoryJsonStr != null) {
                    trajectoryJson.put("trajectory", new JSONArray(trajectoryJsonStr));
                } else {
                    // 如果轨迹数据不存在，添加空数组
                    trajectoryJson.put("trajectory", new JSONArray());
                }

                // 批量导入HBase
                batchImportToHBase(trajectoryJson);

                // 清理Redis数据
                cleanupVehicle(jedis, id);
            }
        }

        private void cleanupVehicle(Jedis jedis, String id) {
            // 删除所有相关Redis键
            jedis.del(REDIS_METADATA_PREFIX + id);
            jedis.del(REDIS_TRAJECTORY_PREFIX + id);
            jedis.del(REDIS_LAST_SEEN_PREFIX + id);
            jedis.del(REDIS_LAST_SAMPLE_PREFIX + id);
        }

        // 安全获取方法
        private int getDirectionSafely(JSONObject tdata) {
            try { return tdata.getInt("direction"); }
            catch (JSONException e) { return -1; }
        }
    }
}