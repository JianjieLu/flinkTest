package whu.edu.moniData.ingest;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.json.JSONArray;
import org.json.JSONObject;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Tuple;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class UnifiedCarTrajectoryJob {

    private static final String REDIS_HOST = "100.65.38.141";
    private static final int REDIS_PORT = 6380;
    private static final String REDIS_PASSWORD = "whdx123cgz666";
    private static final String TRAJECTORY_ZSET_PREFIX = "vehicle_traj:";
    private static final String VEHICLE_ATTR_HASH_PREFIX = "vehicle_attr:";
    private static final long SAMPLING_INTERVAL_MS = 1000; // 1秒采样间隔
    private static final long BULKLOAD_INTERVAL_MS = 15000; // 5分钟批量导入
    private static final int BUCKET_COUNT = 10; // Redis桶数量
    private static final String BUCKET_KEY_PREFIX = "traj_bucket:";
    private static final ReentrantLock tableLock = new ReentrantLock();
    private static final ConcurrentHashMap<String, Object> tableCreationLocks = new ConcurrentHashMap<>();

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(BUCKET_COUNT);
        env.enableCheckpointing(60000); // 1分钟checkpoint

        // Kafka配置
        String brokers = "10.48.53.82:9092";
        String groupId = "flink-trajectory-group";
        List<String> topics = Arrays.asList("fiberData1", "fiberData2", "fiberData3", "fiberData4", "fiberData5",
                "fiberData6", "fiberData7", "fiberData8", "fiberData9", "fiberData10", "fiberData11");

        // 创建Kafka源
        DataStream<String> kafkaStream = createKafkaSource(env, brokers, groupId, topics);

        // 处理轨迹数据
        SingleOutputStreamOperator<Void> processingStream = kafkaStream
                .keyBy(json -> {
                    try {
                        JSONObject obj = new JSONObject(json);
                        JSONArray pathList = obj.getJSONArray("pathList");
                        if (pathList.length() > 0) {
                            return String.valueOf(pathList.getJSONObject(0).getLong("id"));
                        }
                    } catch (Exception e) {
                        // 处理异常
                    }
                    return "default";
                })
                .process(new TrajectorySamplingProcessor());

        // 添加批量导入HBase的定时任务
        env.addSource(new BulkLoadTrigger())
                .keyBy(x -> x % BUCKET_COUNT)
                .process(new RedisToHBaseBulkLoader())
                .name("hbase-bulk-loader");

        env.execute("Unified Car Trajectory Processing");
    }

    private static DataStream<String> createKafkaSource(
            StreamExecutionEnvironment env,
            String brokers,
            String groupId,
            List<String> topics) {

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(topics)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("auto.offset.commit", "true")
                .setProperty("session.timeout.ms", "60000")
                .build();

        return env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
    }

    // 轨迹采样处理器
    private static class TrajectorySamplingProcessor
            extends KeyedProcessFunction<String, String, Void> {

        private transient JedisPool jedisPool;
        private transient ValueState<Long> lastSampleTimeState;

        @Override
        public void open(Configuration parameters) {
            // 初始化Redis连接池
            JedisPoolConfig poolConfig = new JedisPoolConfig();
            poolConfig.setMaxTotal(100);
            poolConfig.setMaxIdle(20);
            poolConfig.setMinIdle(5);
            jedisPool = new JedisPool(poolConfig, REDIS_HOST, REDIS_PORT, 2000, REDIS_PASSWORD);

            // 初始化状态
            lastSampleTimeState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("lastSampleTime", Long.class));
        }

        @Override
        public void processElement(
                String jsonStr,
                Context ctx,
                Collector<Void> out) throws Exception {

            try (Jedis jedis = jedisPool.getResource()) {
                JSONObject json = new JSONObject(jsonStr);
                String timestampStr = json.getString("timeStamp");
                long eventTime = parseTimestamp(timestampStr);
                JSONArray pathList = json.getJSONArray("pathList");

                for (int i = 0; i < pathList.length(); i++) {
                    JSONObject point = pathList.getJSONObject(i);
                    String vehicleId = String.valueOf(point.getLong("id"));

                    // 检查采样间隔
                    Long lastSampleTime = lastSampleTimeState.value();
                    if (lastSampleTime == null || eventTime - lastSampleTime >= SAMPLING_INTERVAL_MS) {
                        // 采样并存储到Redis
                        storeTrajectoryPoint(jedis, vehicleId, eventTime, point);

                        // 更新最后采样时间
                        lastSampleTimeState.update(eventTime);
                    }

                    // 更新车辆属性
                    updateVehicleAttributes(jedis, vehicleId, point);

                    // 添加到处理桶
                    int bucket = Math.abs(vehicleId.hashCode()) % BUCKET_COUNT;
                    jedis.sadd(BUCKET_KEY_PREFIX + bucket, vehicleId);
                }
            }
        }

        private long parseTimestamp(String timestampStr) {
            try {
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS");
                LocalDateTime ldt = LocalDateTime.parse(timestampStr, formatter);
                return ldt.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
            } catch (Exception e) {
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SS");
                LocalDateTime ldt = LocalDateTime.parse(timestampStr, formatter);
                return ldt.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
            }
        }

        private void storeTrajectoryPoint(
                Jedis jedis,
                String vehicleId,
                long timestamp,
                JSONObject point) {

            // 创建轨迹点数据
            JSONObject trajPoint = new JSONObject();
            trajPoint.put("lon", point.getDouble("longitude"));
            trajPoint.put("lat", point.getDouble("latitude"));
            trajPoint.put("lane", point.getInt("laneNo"));
            trajPoint.put("speed", point.getDouble("speed"));
            trajPoint.put("dir", point.optInt("direction", -1));

            // 存储到有序集合
            String zsetKey = TRAJECTORY_ZSET_PREFIX + vehicleId;
            jedis.zadd(zsetKey, timestamp, trajPoint.toString());
        }

        private void updateVehicleAttributes(
                Jedis jedis,
                String vehicleId,
                JSONObject point) {

            String hashKey = VEHICLE_ATTR_HASH_PREFIX + vehicleId;

            // 如果属性不存在则初始化
            if (!jedis.exists(hashKey)) {
                Map<String, String> attrs = new HashMap<>();
                attrs.put("plateNo", point.optString("plateNo", ""));
                attrs.put("type", String.valueOf(point.optInt("vehicleType", 0)));
                attrs.put("color", String.valueOf(point.optInt("vehicleColor", 0)));
                attrs.put("weight", String.valueOf(point.optDouble("vehicleWeight", 0.0)));
                attrs.put("specialFlag", point.optString("specialFlag", "0"));

                jedis.hmset(hashKey, attrs);
            }
        }

        @Override
        public void close() {
            if (jedisPool != null) {
                jedisPool.close();
            }
        }
    }

    // 批量导入触发器
    private static class BulkLoadTrigger
            implements org.apache.flink.streaming.api.functions.source.SourceFunction<Integer> {

        private volatile boolean isRunning = true;

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            int count = 0;
            while (isRunning) {
                // 每5分钟发送一次触发信号
                TimeUnit.MILLISECONDS.sleep(BULKLOAD_INTERVAL_MS);
                ctx.collect(count++);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    // Redis到HBase批量加载器 - 使用高效批导方式
    // Redis到HBase批量加载器 - 使用高效批导方式
    // Redis到HBase批量加载器 - 使用高效批导方式
    // Redis到HBase批量加载器 - 使用高效批导方式
    private static class RedisToHBaseBulkLoader
            extends KeyedProcessFunction<Integer, Integer, Void> {

        private transient JedisPool jedisPool;
        private transient Connection hbaseConn;
        private transient Admin hbaseAdmin;
        private org.apache.hadoop.conf.Configuration hbaseConf;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 初始化Redis连接池
            JedisPoolConfig redisConfig = new JedisPoolConfig();
            redisConfig.setMaxTotal(50);
            jedisPool = new JedisPool(redisConfig, REDIS_HOST, REDIS_PORT, 2000, REDIS_PASSWORD);

            // 初始化HBase配置
            hbaseConf = HBaseConfiguration.create();
            hbaseConf.set("hbase.zookeeper.quorum", "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142,10.48.53.80");
            hbaseConf.set("hbase.zookeeper.property.clientPort", "2181");
            hbaseConf.setInt("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", 64);
            hbaseConf.setInt("hbase.client.operation.timeout", 120000); // 2分钟超时

            hbaseConn = ConnectionFactory.createConnection(hbaseConf);
            hbaseAdmin = hbaseConn.getAdmin();
        }

        @Override
        public void processElement(
                Integer trigger,
                Context ctx,
                Collector<Void> out) throws Exception {

            int bucketId = ctx.getCurrentKey();
            String bucketKey = BUCKET_KEY_PREFIX + bucketId;

            try (Jedis jedis = jedisPool.getResource()) {
                // 获取当前桶中的所有车辆ID
                Set<String> vehicleIds = jedis.smembers(bucketKey);
                if (vehicleIds == null || vehicleIds.isEmpty()) return;

                // 按日期表分组收集KeyValue
                Map<String, List<KeyValue>> tableKeyValues = new HashMap<>();
                Map<String, String> rowToTableMap = new HashMap<>();

                // 处理每个车辆的轨迹数据
                for (String vehicleId : vehicleIds) {
                    processVehicleTrajectory(jedis, vehicleId, tableKeyValues, rowToTableMap);
                }

                // 清空当前桶
                jedis.del(bucketKey);

                // 批量加载每个表的HFiles
                for (Map.Entry<String, List<KeyValue>> entry : tableKeyValues.entrySet()) {
                    String tableName = entry.getKey();
                    List<KeyValue> keyValues = entry.getValue();

                    // 确保表存在
                    createTableIfNotExists(tableName, "cf0", hbaseConn);

                    // 排序数据
                    sortKeyValues(keyValues);

                    // 批导入HBase
                    bulkLoadToHBase(tableName, keyValues);

                    System.out.println("批量加载完成，表: " + tableName +
                            ", 记录数: " + keyValues.size()/5); // 每条记录5个KeyValue
                }
            } catch (Exception e) {
                System.err.println("批导过程失败: " + e.getMessage());
                throw e;
            }
        }

        private void processVehicleTrajectory(
                Jedis jedis,
                String vehicleId,
                Map<String, List<KeyValue>> tableKeyValues,
                Map<String, String> rowToTableMap) throws Exception {

            // 1. 从Redis获取轨迹数据
            String zsetKey = TRAJECTORY_ZSET_PREFIX + vehicleId;
            String hashKey = VEHICLE_ATTR_HASH_PREFIX + vehicleId;

            // 获取所有轨迹点
            Set<Tuple> points = jedis.zrangeWithScores(zsetKey, 0, -1);
            if (points == null || points.isEmpty()) return;

            // 获取车辆属性
            Map<String, String> attrs = jedis.hgetAll(hashKey);
            if (attrs == null) return;

            // 2. 准备HBase数据
            String plateNo = attrs.getOrDefault("plateNo", "UNKNOWN");
            long minTimestamp = Long.MAX_VALUE;
            JSONArray trajectory = new JSONArray();

            for (Tuple point : points) {
                JSONObject pointJson = new JSONObject(point.getElement());
                long timestamp = (long) point.getScore();

                // 更新最小时间戳
                if (timestamp < minTimestamp) {
                    minTimestamp = timestamp;
                }

                trajectory.put(pointJson);
            }

            // 3. 确定行键和表名
            String rowKey = minTimestamp + "-" + plateNo + "-" + vehicleId;
            String dateSuffix = getDateSuffix(minTimestamp);
            String tableName = "ZCarTraj_" + dateSuffix;
            rowToTableMap.put(rowKey, tableName);

            // 4. 创建KeyValue集合
            List<KeyValue> keyValues = tableKeyValues.computeIfAbsent(tableName, k -> new ArrayList<>());
            long now = System.currentTimeMillis();

            // 每个字段生成一个KeyValue
            keyValues.add(createKeyValue(rowKey, "cf0", "plate", now,
                    attrs.getOrDefault("plateNo", "")));
            keyValues.add(createKeyValue(rowKey, "cf0", "type", now + 1,
                    attrs.getOrDefault("type", "0")));
            keyValues.add(createKeyValue(rowKey, "cf0", "color", now + 2,
                    attrs.getOrDefault("color", "0")));
            keyValues.add(createKeyValue(rowKey, "cf0", "weight", now + 3,
                    attrs.getOrDefault("weight", "0.0")));
            keyValues.add(createKeyValue(rowKey, "cf0", "specialFlag", now + 4,
                    attrs.getOrDefault("specialFlag", "0")));
            keyValues.add(createKeyValue(rowKey, "cf0", "trajectory", now + 5,
                    trajectory.toString()));

            // 5. 清理Redis数据
            jedis.del(zsetKey);
            jedis.del(hashKey);
        }

        private KeyValue createKeyValue(
                String rowKey,
                String cf,
                String qualifier,
                long timestamp,
                String value) {

            return new KeyValue(
                    Bytes.toBytes(rowKey),
                    Bytes.toBytes(cf),
                    Bytes.toBytes(qualifier),
                    timestamp,
                    Bytes.toBytes(value)
            );
        }

        private void sortKeyValues(List<KeyValue> keyValues) {
            keyValues.sort((kv1, kv2) -> {
                int rowCompare = Bytes.compareTo(kv1.getRow(), kv2.getRow());
                if (rowCompare != 0) return rowCompare;

                int familyCompare = Bytes.compareTo(kv1.getFamily(), kv2.getFamily());
                if (familyCompare != 0) return familyCompare;

                int qualifierCompare = Bytes.compareTo(kv1.getQualifier(), kv2.getQualifier());
                if (qualifierCompare != 0) return qualifierCompare;

                return Long.compare(kv2.getTimestamp(), kv1.getTimestamp()); // 降序排序
            });
        }

        private void bulkLoadToHBase(String tableName, List<KeyValue> keyValues)
                throws IOException, InterruptedException {

            // 1. 创建临时目录
            String outputDir = "hdfs:///tmp/hfiles/" + tableName + "_" +
                    System.currentTimeMillis() + "_" + UUID.randomUUID();
            Path outputPath = new Path(outputDir);
            Path outputPathForFamily = new Path(outputDir, "cf0"); // 列族目录

            try {
                // 2. 创建HFile
                createHFiles(keyValues, outputPathForFamily, tableName);

                // 3. 加载HFiles到HBase - 使用LoadIncrementalHFiles替代BulkLoadHFiles
                LoadIncrementalHFiles loader = new LoadIncrementalHFiles(hbaseConf);

                // 设置加载线程数
                hbaseConf.setInt("hbase.loadincremental.threads.max", 8);

                // 执行批量加载
                TableName hbaseTableName = TableName.valueOf(tableName);
                try (Connection conn = ConnectionFactory.createConnection(hbaseConf);
                     Table table = conn.getTable(hbaseTableName); // 获取Table对象
                     RegionLocator locator = conn.getRegionLocator(hbaseTableName)) {

                    loader.doBulkLoad(outputPath, hbaseAdmin, table, locator);
                    System.out.println("成功加载表: " + tableName + ", HFile数量: " + keyValues.size());
                }

            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                // 4. 清理临时文件
                try {
                    outputPath.getFileSystem(hbaseConf).delete(outputPath, true);
                } catch (IOException e) {
                    System.err.println("清理临时文件失败: " + e.getMessage());
                }
            }
        }

        private void createHFiles(
                List<KeyValue> keyValues,
                Path outputPath,
                String tableName) throws IOException {

            HFileContext context = new HFileContextBuilder()
                    .withBlockSize(64 * 1024) // 64KB块大小
                    .build();

            // 获取文件系统
            FileSystem fs = outputPath.getFileSystem(hbaseConf);

            // 创建HFile Writer - 使用正确的参数
            HFile.Writer writer = HFile.getWriterFactory(hbaseConf, new CacheConfig(hbaseConf))
                    .withPath(fs, outputPath) // 只需要两个参数：FileSystem和Path
                    .withFileContext(context)
                    .create();

            try {
                // 写入KeyValue
                for (KeyValue kv : keyValues) {
                    writer.append(kv);
                }
            } finally {
                writer.close();
            }
        }

        private String getDateSuffix(long timestamp) {
            LocalDate date = Instant.ofEpochMilli(timestamp)
                    .atZone(ZoneId.systemDefault())
                    .toLocalDate();
            return date.format(DateTimeFormatter.BASIC_ISO_DATE); // yyyyMMdd
        }

        @Override
        public void close() throws Exception {
            if (jedisPool != null) jedisPool.close();
            if (hbaseAdmin != null) hbaseAdmin.close();
            if (hbaseConn != null) hbaseConn.close();
        }
    }

    private static void createTableIfNotExists(String tableName, String columnFamily, Connection connection) {
        tableLock.lock();
        try (Admin admin = connection.getAdmin()) {
            TableName hbaseTableName = TableName.valueOf(tableName);

            Object lock = tableCreationLocks.computeIfAbsent(tableName, k -> new Object());

            synchronized (lock) {
                admin.listTables();
                if (!admin.tableExists(hbaseTableName)) {
                    HTableDescriptor tableDescriptor = new HTableDescriptor(hbaseTableName);
                    tableDescriptor.addFamily(new HColumnDescriptor(columnFamily));
                    try {
                        admin.createTable(tableDescriptor);
                        System.out.println("表创建成功: " + tableName);
                    } catch (TableExistsException e) {
                        // 处理表已存在的情况
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            tableLock.unlock();
        }
    }
}