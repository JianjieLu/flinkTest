package whu.edu.moniData.ingest;


import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.json.*;
import redis.clients.jedis.*;

import java.io.IOException;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class VehicleTrajectoryPipeline {

    // Redis配置
    private static final String REDIS_HOST = "100.65.38.141";
    private static final int REDIS_PORT = 6380;
    private static final String REDIS_PASSWORD = "your_redis_password";
    private static final int REDIS_MAX_TOTAL = 200;
    private static final int REDIS_MAX_IDLE = 50;

    // Kafka配置
    private static final String KAFKA_BROKERS = "10.48.53.82:9092";
    private static final String KAFKA_GROUP_ID = "flink-vehicle-group";
    private static final List<String> KAFKA_TOPICS = Arrays.asList(
            "fiberData1", "fiberData2", "fiberData3", "fiberData4", "fiberData5",
            "fiberData6", "fiberData7", "fiberData8", "fiberData9", "fiberData10", "fiberData11"
    );

    // 时间配置
    private static final long SAMPLING_INTERVAL_MS = 1000;    // 1秒采样间隔
    private static final long BULKLOAD_INTERVAL_MS = 300000;  // 5分钟批量导入
    private static final long STATE_TIMEOUT_MS = 600000;      // 10分钟无数据超时

    // Redis数据结构
    private static final String TRAJECTORY_KEY_PREFIX = "vehicle_traj:";
    private static final String ATTRIBUTES_KEY_PREFIX = "vehicle_attr:";
    private static final String BUCKET_SET_KEY = "vehicle_buckets";

    // HBase配置
    private static final String HBASE_ZOOKEEPER_QUORUM = "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142";
    private static final String HBASE_ZOOKEEPER_PORT = "2181";
    private static final String HBASE_TABLE_PREFIX = "ZCarTraj_";

    // 错误输出标签
    private static final OutputTag<String> ERROR_TAG = new OutputTag<String>("errors") {};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(10);  // 根据实际环境调整并行度

        // 创建Kafka数据源
        DataStream<String> kafkaStream = createKafkaSource(env);

        // 数据处理管道
        SingleOutputStreamOperator<VehicleData> parsedStream = kafkaStream
                .process(new KafkaParser())
                .name("kafka-parser");

        // 获取错误流
        DataStream<String> errorStream = parsedStream.getSideOutput(ERROR_TAG);
        errorStream.print("Parse Errors");

        parsedStream
                .keyBy(data -> Math.abs(data.getVehicleId().hashCode()) % 10)  // 按车辆ID分桶
                .process(new TrajectorySampler())
                .name("trajectory-sampler")
                .addSink(new BulkLoadToHBase())
                .name("hbase-sink");

        env.execute("Vehicle Trajectory Processing Pipeline");
    }

    private static DataStream<String> createKafkaSource(StreamExecutionEnvironment env) {
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(KAFKA_BROKERS)
                .setTopics(KAFKA_TOPICS)
                .setGroupId(KAFKA_GROUP_ID)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("session.timeout.ms", "60000")
                .setProperty("auto.offset.reset", "latest")
                .build();

        return env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
    }

    // Kafka数据解析器
    private static class KafkaParser extends ProcessFunction<String, VehicleData> {
        private static final DateTimeFormatter TS_FORMAT =
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[:SS][:S]");

        @Override
        public void processElement(String value, Context ctx, Collector<VehicleData> out) {
            try {
                JSONObject json = new JSONObject(value);
                String timestampStr = json.getString("timeStamp");
                JSONArray pathList = json.getJSONArray("pathList");

                // 解析时间戳
                long timestamp = parseTimestamp(timestampStr);

                for (int i = 0; i < pathList.length(); i++) {
                    JSONObject point = pathList.getJSONObject(i);
                    VehicleData data = new VehicleData();
                    data.setTimestamp(timestamp);
                    data.setVehicleId(point.getString("id"));
                    data.setPlateNo(point.getString("plateNo"));
                    data.setLongitude(point.getDouble("longitude"));
                    data.setLatitude(point.getDouble("latitude"));
                    data.setLaneNo(point.getInt("laneNo"));
                    data.setSpeed(point.getDouble("speed"));
                    data.setVehicleType(point.getInt("vehicleType"));
                    data.setDirection(point.optInt("direction", -1));
                    data.setSpecialFlag(point.optString("specialFlag", "0"));

                    out.collect(data);
                }
            } catch (Exception e) {
                ctx.output(ERROR_TAG, "Parse Error: " + value + " - " + e.getMessage());
            }
        }

        private long parseTimestamp(String timestampStr) {
            try {
                LocalDateTime ldt = LocalDateTime.parse(timestampStr, TS_FORMAT);
                return ldt.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
            } catch (Exception e) {
                // 尝试其他格式
                try {
                    return Instant.parse(timestampStr).toEpochMilli();
                } catch (Exception ex) {
                    throw new DateTimeException("Invalid timestamp format: " + timestampStr);
                }
            }
        }
    }

    // 轨迹采样处理器
    private static class TrajectorySampler extends KeyedProcessFunction<Integer, VehicleData, TrajectoryBatch> {
        private transient JedisPool jedisPool;
        private transient Map<String, Long> lastSampleTimes;
        private transient AtomicLong nextBulkLoadTime;
        private transient ScheduledExecutorService scheduler;
        private final Map<Integer, Boolean> timerRegistered = new ConcurrentHashMap<>();

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            // 初始化Redis连接池
            JedisPoolConfig poolConfig = new JedisPoolConfig();
            poolConfig.setMaxTotal(REDIS_MAX_TOTAL);
            poolConfig.setMaxIdle(REDIS_MAX_IDLE);
            poolConfig.setTestOnBorrow(true);
            jedisPool = new JedisPool(poolConfig, REDIS_HOST, REDIS_PORT, 2000, REDIS_PASSWORD);

            lastSampleTimes = new ConcurrentHashMap<>();
            nextBulkLoadTime = new AtomicLong(System.currentTimeMillis() + BULKLOAD_INTERVAL_MS);
            scheduler = Executors.newSingleThreadScheduledExecutor();

            // 定时调度批量处理任务
            scheduler.scheduleAtFixedRate(this::triggerBulkLoad,
                    BULKLOAD_INTERVAL_MS, BULKLOAD_INTERVAL_MS, TimeUnit.MILLISECONDS);
        }

        @Override
        public void processElement(VehicleData data, Context ctx, Collector<TrajectoryBatch> out) {
            try (Jedis jedis = jedisPool.getResource()) {
                String vehicleId = data.getVehicleId();
                long currentTime = data.getTimestamp();

                // 检查是否需要进行采样
                long lastSampleTime = lastSampleTimes.getOrDefault(vehicleId, 0L);
                if (currentTime - lastSampleTime >= SAMPLING_INTERVAL_MS) {
                    // 存储轨迹点
                    storeTrajectoryPoint(jedis, data);

                    // 更新车辆属性
                    updateVehicleAttributes(jedis, data);

                    // 更新最后采样时间
                    lastSampleTimes.put(vehicleId, currentTime);
                }

                // 清理超时状态
                evictStaleStates(currentTime);
            } catch (Exception e) {
                System.err.println("Redis Error: " + e.getMessage());
            }
        }

        private void storeTrajectoryPoint(Jedis jedis, VehicleData data) {
            String key = TRAJECTORY_KEY_PREFIX + data.getVehicleId();
            JSONObject point = new JSONObject();
            point.put("ts", data.getTimestamp());
            point.put("lng", data.getLongitude());
            point.put("lat", data.getLatitude());
            point.put("lane", data.getLaneNo());
            point.put("speed", data.getSpeed());

            // 使用有序集合存储轨迹点
            jedis.zadd(key, data.getTimestamp(), point.toString());

            // 添加车辆到处理桶
            int bucket = Math.abs(data.getVehicleId().hashCode()) % 10;
            jedis.sadd(BUCKET_SET_KEY + ":" + bucket, data.getVehicleId());
        }

        private void updateVehicleAttributes(Jedis jedis, VehicleData data) {
            String key = ATTRIBUTES_KEY_PREFIX + data.getVehicleId();
            Map<String, String> attributes = new HashMap<>();
            attributes.put("plateNo", data.getPlateNo());
            attributes.put("type", String.valueOf(data.getVehicleType()));
            attributes.put("specialFlag", data.getSpecialFlag());
            attributes.put("direction", String.valueOf(data.getDirection()));

            jedis.hset(key, attributes);
        }

        private void evictStaleStates(long currentTime) {
            Iterator<Map.Entry<String, Long>> it = lastSampleTimes.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, Long> entry = it.next();
                if (currentTime - entry.getValue() > STATE_TIMEOUT_MS) {
                    it.remove();
                }
            }
        }

        private void triggerBulkLoad() {
            try (Jedis jedis = jedisPool.getResource()) {
                // 遍历所有桶
                for (int bucket = 0; bucket < 10; bucket++) {
                    String bucketKey = BUCKET_SET_KEY + ":" + bucket;

                    // 获取桶内的所有车辆ID
                    Set<String> vehicleIds = jedis.smembers(bucketKey);
                    if (vehicleIds == null || vehicleIds.isEmpty()) continue;

                    // 处理每个车辆
                    for (String vehicleId : vehicleIds) {
                        try {
                            // 获取时间范围内的轨迹点
                            long endTime = System.currentTimeMillis();
                            long startTime = endTime - BULKLOAD_INTERVAL_MS;

                            String trajKey = TRAJECTORY_KEY_PREFIX + vehicleId;
                            Set<Tuple> points = jedis.zrangeByScoreWithScores(trajKey, startTime, endTime);

                            if (points != null && !points.isEmpty()) {
                                // 获取车辆属性
                                String attrKey = ATTRIBUTES_KEY_PREFIX + vehicleId;
                                Map<String, String> attributes = jedis.hgetAll(attrKey);

                                // 创建轨迹批次
                                TrajectoryBatch batch = new TrajectoryBatch();
                                batch.setVehicleId(vehicleId);
                                batch.setPlateNo(attributes.getOrDefault("plateNo", "UNKNOWN"));
                                batch.setVehicleType(Integer.parseInt(attributes.getOrDefault("type", "0")));
                                batch.setSpecialFlag(attributes.getOrDefault("specialFlag", "0"));

                                // 添加轨迹点
                                for (Tuple tuple : points) {
                                    JSONObject pointJson = new JSONObject(tuple.getElement());
                                    TrajectoryPoint point = new TrajectoryPoint();
                                    point.setTimestamp(pointJson.getLong("ts"));
                                    point.setLongitude(pointJson.getDouble("lng"));
                                    point.setLatitude(pointJson.getDouble("lat"));
                                    point.setLaneNo(pointJson.getInt("lane"));
                                    point.setSpeed(pointJson.getDouble("speed"));
                                    batch.addPoint(point);
                                }

                                // 发送批次数据
                                // 在实际应用中，这里应该将批次发送到输出收集器
                                // out.collect(batch);

                                // 删除已处理的轨迹点
                                jedis.zremrangeByScore(trajKey, startTime, endTime);
                            }
                        } catch (Exception e) {
                            System.err.println("Error processing vehicle: " + vehicleId + " - " + e.getMessage());
                        }
                    }

                    // 清空桶
                    jedis.del(bucketKey);
                }
            } catch (Exception e) {
                System.err.println("BulkLoad Trigger Error: " + e.getMessage());
            }

            // 更新下次批量处理时间
            nextBulkLoadTime.set(System.currentTimeMillis() + BULKLOAD_INTERVAL_MS);
        }

        @Override
        public void close() {
            if (scheduler != null) {
                scheduler.shutdown();
            }
            if (jedisPool != null) {
                jedisPool.close();
            }
        }
    }

private static class BulkLoadToHBase extends RichSinkFunction<TrajectoryBatch> {
        private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd");
        private transient Configuration hbaseConfig;
        private transient Connection hbaseConn;
        private transient Map<String, BufferedMutator> mutatorMap;
        private final Map<String, Long> lastFlushTime = new ConcurrentHashMap<>();
        private static final long FLUSH_INTERVAL = 60000; // 每分钟刷新一次
        private final ReentrantLock tableLock = new ReentrantLock();
        private static final ConcurrentHashMap<String, Object> tableCreationLocks = new ConcurrentHashMap<>();
        private static final String[] COLUMN_FAMILIES = new String[]{"attr", "traj"};

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            super.open(parameters);
            hbaseConfig = HBaseConfiguration.create();
            hbaseConfig.set("hbase.zookeeper.quorum", HBASE_ZOOKEEPER_QUORUM);
            hbaseConfig.set("hbase.zookeeper.property.clientPort", HBASE_ZOOKEEPER_PORT);

            hbaseConn = ConnectionFactory.createConnection(hbaseConfig);
            mutatorMap = new ConcurrentHashMap<>();
        }

        @Override
        public void invoke(TrajectoryBatch batch, Context context) throws Exception {
            String tableName = HBASE_TABLE_PREFIX + getDateSuffix(batch.getMinTimestamp());
            createTableIfNotExists(tableName, COLUMN_FAMILIES);

            BufferedMutator mutator = mutatorMap.computeIfAbsent(tableName, key -> {
                try {
                    BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(tableName))
                            .writeBufferSize(32 * 1024 * 1024); // 32MB buffer
                    return hbaseConn.getBufferedMutator(params);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to create BufferedMutator for table: " + tableName, e);
                }
            });

            Put put = createPut(batch);
            mutator.mutate(put);

            // 定期刷新
            long currentTime = System.currentTimeMillis();
            Long lastFlush = lastFlushTime.get(tableName);
            if (lastFlush == null || currentTime - lastFlush > FLUSH_INTERVAL) {
                mutator.flush();
                lastFlushTime.put(tableName, currentTime);
            }
        }

        private Put createPut(TrajectoryBatch batch) {
            String rowKey = batch.getMinTimestamp() + "-" + batch.getPlateNo() + "-" + batch.getVehicleId();
            Put put = new Put(Bytes.toBytes(rowKey));

            // 添加属性列
            put.addColumn(Bytes.toBytes("attr"), Bytes.toBytes("type"),
                    Bytes.toBytes(String.valueOf(batch.getVehicleType())));
            put.addColumn(Bytes.toBytes("attr"), Bytes.toBytes("plate"),
                    Bytes.toBytes(batch.getPlateNo()));
            put.addColumn(Bytes.toBytes("attr"), Bytes.toBytes("special"),
                    Bytes.toBytes(batch.getSpecialFlag()));

            // 添加轨迹列
            JSONArray trajArray = new JSONArray();
            for (TrajectoryPoint point : batch.getPoints()) {
                JSONObject pointObj = new JSONObject();
                pointObj.put("ts", point.getTimestamp());
                pointObj.put("lng", point.getLongitude());
                pointObj.put("lat", point.getLatitude());
                pointObj.put("lane", point.getLaneNo());
                pointObj.put("speed", point.getSpeed());
                trajArray.put(pointObj);
            }

            put.addColumn(Bytes.toBytes("traj"), Bytes.toBytes("points"),
                    Bytes.toBytes(trajArray.toString()));

            return put;
        }

        private String getDateSuffix(long timestamp) {
            LocalDate date = Instant.ofEpochMilli(timestamp)
                    .atZone(ZoneId.systemDefault())
                    .toLocalDate();
            return date.format(DATE_FORMAT);
        }

        public void createTableIfNotExists(String tableName, String[] columnFamily) throws IOException {
            tableLock.lock();
            try (Admin admin = hbaseConn.getAdmin()) {
                TableName hbaseTableName = TableName.valueOf(tableName);
                Object lock = tableCreationLocks.computeIfAbsent(tableName, k -> new Object());

                synchronized (lock) {
                    if (!admin.tableExists(hbaseTableName)) {
                        HTableDescriptor tableDescriptor = new HTableDescriptor(hbaseTableName);
                        for (String cf : columnFamily) {
                            tableDescriptor.addFamily(new HColumnDescriptor(cf));
                        }
                        admin.createTable(tableDescriptor);
                    }
                    // 清理锁对象防止内存泄漏
                    tableCreationLocks.remove(tableName);
                }
            } finally {
                tableLock.unlock();
            }
        }

        @Override
        public void close() throws Exception {
            // 1. 先刷新并关闭所有BufferedMutator
            for (BufferedMutator mutator : mutatorMap.values()) {
                try {
                    mutator.flush();
                    mutator.close();
                } catch (IOException e) {
                    System.err.println("Error closing mutator: " + e.getMessage());
                }
            }
            mutatorMap.clear();

            // 2. 关闭HBase连接
            if (hbaseConn != null && !hbaseConn.isClosed()) {
                try {
                    hbaseConn.close();
                } catch (IOException e) {
                    System.err.println("Error closing HBase connection: " + e.getMessage());
                }
            }

            super.close();
        }
    }

    // ====================== 数据模型类 ======================

    private static class VehicleData {
        private String vehicleId;
        private String plateNo;
        private long timestamp;
        private double longitude;
        private double latitude;
        private int laneNo;
        private double speed;
        private int vehicleType;
        private int direction;
        private String specialFlag;

        // Getters and setters
        public String getVehicleId() { return vehicleId; }
        public void setVehicleId(String vehicleId) { this.vehicleId = vehicleId; }
        public String getPlateNo() { return plateNo; }
        public void setPlateNo(String plateNo) { this.plateNo = plateNo; }
        public long getTimestamp() { return timestamp; }
        public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
        public double getLongitude() { return longitude; }
        public void setLongitude(double longitude) { this.longitude = longitude; }
        public double getLatitude() { return latitude; }
        public void setLatitude(double latitude) { this.latitude = latitude; }
        public int getLaneNo() { return laneNo; }
        public void setLaneNo(int laneNo) { this.laneNo = laneNo; }
        public double getSpeed() { return speed; }
        public void setSpeed(double speed) { this.speed = speed; }
        public int getVehicleType() { return vehicleType; }
        public void setVehicleType(int vehicleType) { this.vehicleType = vehicleType; }
        public int getDirection() { return direction; }
        public void setDirection(int direction) { this.direction = direction; }
        public String getSpecialFlag() { return specialFlag; }
        public void setSpecialFlag(String specialFlag) { this.specialFlag = specialFlag; }
    }

    private static class TrajectoryPoint {
        private long timestamp;
        private double longitude;
        private double latitude;
        private int laneNo;
        private double speed;

        // Getters and setters
        public long getTimestamp() { return timestamp; }
        public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
        public double getLongitude() { return longitude; }
        public void setLongitude(double longitude) { this.longitude = longitude; }
        public double getLatitude() { return latitude; }
        public void setLatitude(double latitude) { this.latitude = latitude; }
        public int getLaneNo() { return laneNo; }
        public void setLaneNo(int laneNo) { this.laneNo = laneNo; }
        public double getSpeed() { return speed; }
        public void setSpeed(double speed) { this.speed = speed; }
    }

    private static class TrajectoryBatch {
        private String vehicleId;
        private String plateNo;
        private int vehicleType;
        private String specialFlag;
        private List<TrajectoryPoint> points = new ArrayList<>();

        public void addPoint(TrajectoryPoint point) {
            points.add(point);
        }

        public long getMinTimestamp() {
            return points.stream()
                    .mapToLong(TrajectoryPoint::getTimestamp)
                    .min()
                    .orElse(System.currentTimeMillis());
        }

        // Getters and setters
        public String getVehicleId() { return vehicleId; }
        public void setVehicleId(String vehicleId) { this.vehicleId = vehicleId; }
        public String getPlateNo() { return plateNo; }
        public void setPlateNo(String plateNo) { this.plateNo = plateNo; }
        public int getVehicleType() { return vehicleType; }
        public void setVehicleType(int vehicleType) { this.vehicleType = vehicleType; }
        public String getSpecialFlag() { return specialFlag; }
        public void setSpecialFlag(String specialFlag) { this.specialFlag = specialFlag; }
        public List<TrajectoryPoint> getPoints() { return points; }
    }

    // HBase BulkLoad工具类
    private static class HBaseBulkLoader {
        public static void bulkLoad(Configuration conf, Path outputPath, TableName tableName)
                throws Exception {

            try (Connection conn = ConnectionFactory.createConnection(conf);
                 Admin admin = conn.getAdmin();
                 Table table = conn.getTable(tableName);
                 RegionLocator regionLocator = conn.getRegionLocator(tableName)) {

                // 配置MapReduce作业
                Job job = Job.getInstance(conf, "HBaseBulkLoad-" + tableName);
                job.setJarByClass(HBaseBulkLoader.class);
                HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator);
                HFileOutputFormat2.setOutputPath(job, outputPath);

                // 执行批量加载
                LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
                loader.doBulkLoad(outputPath, admin, table, regionLocator);
            }
        }
    }
}