package whu.edu.moniData.ingest.holyAnalysisJob;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class CombinedCarTrajStorageWithRedis {

    // Redis配置
    private static final String REDIS_HOST = "100.65.38.141";
    private static final int REDIS_PORT = 6380;
    private static final String REDIS_PASSWORD = "whdx123cgz666";
    private static final String REDIS_KEY_PREFIX = "vehicle_trajectory:";

    // HBase配置
    private static final String HBASE_ZOOKEEPER_QUORUM = "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142,10.48.53.80";
    private static final String HBASE_ZOOKEEPER_PORT = "2181";
    private static final String HBASE_BASE_TABLE_NAME = "vehicle_trajectories";
    private static final String HBASE_COLUMN_FAMILY = "cf";

    // 车辆数据点类
    @Data
    @AllArgsConstructor
    public static class VehicleDataPoint {
        private String plateNo;
        private int vehicleType;
        private double speed;
        private int laneNo;
        private double longitude;
        private double latitude;
        private String stakeId;
        private long timestamp;
        private double vehicleWeight;
    }

    // 车辆轨迹类
    @Data
    public static class VehicleTrajectory {
        private String plateNo;
        private int vehicleType;
        private List<Map<String, Object>> points = new ArrayList<>();
        private long startTime;
        private long endTime;
        private int totalPoints;
        private double avgSpeed;
        private double totalDistance;

        @Override
        public String toString() {
            return String.format("VehicleTrajectory{plateNo='%s', points=%d, startTime=%d, endTime=%d}",
                    plateNo, totalPoints, startTime, endTime);
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("开始启动车辆轨迹处理任务...");
        // 创建执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        System.out.println("执行环境创建完成，并行度: " + env.getParallelism());

        // Kafka配置
        String brokers = "10.48.53.82:9092";
        String groupId = "vehicle-trajectory-group";

        List<String> topics  = Arrays.asList("fiberData1","fiberData2","fiberData3","fiberData4","fiberData5",
                "fiberData6","fiberData7","fiberData8","fiberData9","fiberData10","fiberData11");
        System.out.println("从Kafka主题读取数据: " + topics);

        // 从Kafka读取数据
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(topics)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("max.partition.fetch.bytes", "629145600")
                .build();
        DataStreamSource<String> kafkaStream = env.fromSource(
                source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        System.out.println("Kafka源创建完成");

        // 添加限流控制 - 每秒处理一条消息
        DataStream<String> throttledStream = kafkaStream
                .keyBy(value -> 0) // 所有数据使用同一个key
                .process(new ThrottleFunction(1000)); // 1000毫秒间隔
        System.out.println("限流处理器添加完成（每秒一条消息）");

        // 解析JSON数据并创建VehicleDataPoint
        SingleOutputStreamOperator<VehicleDataPoint> dataStream = throttledStream
                .flatMap((String value, Collector<VehicleDataPoint> out) -> {
                    try {
                        JSONObject json = JSON.parseObject(value);
                        JSONArray pathList = json.getJSONArray("pathList");
                        long timestamp = parseTimestamp(json.getString("timeStamp"));
                        int pointCount = 0;

                        for (int i = 0; i < pathList.size(); i++) {
                            JSONObject vehicle = pathList.getJSONObject(i);
                            VehicleDataPoint point = new VehicleDataPoint(
                                    vehicle.getString("plateNo"),
                                    vehicle.getIntValue("vehicleType"),
                                    vehicle.getDouble("speed"),
                                    vehicle.getIntValue("laneNo"),
                                    vehicle.getDouble("longitude"),
                                    vehicle.getDouble("latitude"),
                                    vehicle.getString("stakeId"),
                                    timestamp,
                                    vehicle.getDouble("vehicleWeight")
                            );
                            out.collect(point);
                            pointCount++;
                        }

                        System.out.println("解析JSON数据成功，包含 " + pointCount + " 个数据点");
                    } catch (Exception e) {
                        System.err.println("解析JSON数据错误: " + e.getMessage());
                        System.err.println("错误数据内容: " + value);
                    }
                })
                .returns(TypeInformation.of(VehicleDataPoint.class))
                // 添加时间戳和水印分配
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<VehicleDataPoint>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
                );

// 按键分区（车牌号）
        KeyedStream<VehicleDataPoint, String> keyedStream = dataStream
                .keyBy(VehicleDataPoint::getPlateNo);
        System.out.println("按键分区完成（车牌号）");

        // 每辆车生成轨迹
        SingleOutputStreamOperator<VehicleTrajectory> trajectoryStream = keyedStream
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.minutes(5)))
                .process(new TrajectoryProcessWindowFunction());
        System.out.println("轨迹生成窗口设置完成（5分钟）");

        // 创建Redis存储处理器
        DataStream<Tuple2<String, VehicleTrajectory>> redisStream = trajectoryStream
                .map(trajectory -> {
                    System.out.println("生成轨迹: " + trajectory);
                    return Tuple2.of(trajectory.getPlateNo(), trajectory);
                })
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, VehicleTrajectory>>() {}));
        System.out.println("Redis存储处理器创建完成");

        // 按车牌号分组
        redisStream.keyBy(t -> t.f0)
                .process(new RedisStorageProcessor());
        System.out.println("按车牌号分组完成");

        // 创建定时触发器（每5分钟触发一次）
        DataStream<Long> triggerStream = env.fromSequence(0, Long.MAX_VALUE)
                .keyBy(x -> 0)
                .process(new TimerTriggerFunction(5 * 60 * 1000)); // 每5分钟触发一次
        System.out.println("定时触发器创建完成（每5分钟触发一次）");

        // 处理定时触发，将Redis数据存入HBase
        triggerStream
                .keyBy(x -> 0)
                .process(new RedisToHBaseProcessor())
                .print(); // 打印处理结果

        // 执行任务
        System.out.println("开始执行任务...");
        env.execute("Vehicle Trajectory Processing with Redis and Dynamic HBase");
        System.out.println("任务执行完成");
    }

    // 限流函数 - 每秒处理一条消息
    private static class ThrottleFunction extends KeyedProcessFunction<Integer, String, String> {
        private final long interval; // 处理间隔（毫秒）
        private transient ListState<String> bufferState;
        private transient ValueState<Long> nextTimerState;
        private transient ValueState<Long> lastProcessTimeState;

        public ThrottleFunction(long interval) {
            this.interval = interval;
        }

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            super.open(parameters); // 必须调用父类方法

            ListStateDescriptor<String> bufferDesc = new ListStateDescriptor<>("buffer", String.class);
            bufferState = getRuntimeContext().getListState(bufferDesc);

            ValueStateDescriptor<Long> timerDesc = new ValueStateDescriptor<>("next-timer", Long.class);
            nextTimerState = getRuntimeContext().getState(timerDesc);

            ValueStateDescriptor<Long> timeDesc = new ValueStateDescriptor<>("last-process-time", Long.class);
            lastProcessTimeState = getRuntimeContext().getState(timeDesc);
        }

        @Override
        public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
            // 添加到缓冲区
            bufferState.add(value);

            // 如果没有定时器，注册一个
            if (nextTimerState.value() == null) {
                long nextTimer = System.currentTimeMillis() + interval;
                ctx.timerService().registerProcessingTimeTimer(nextTimer);
                nextTimerState.update(nextTimer);
                System.out.println("注册第一个限流定时器: " + nextTimer);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 检查缓冲区是否有数据
            Iterator<String> iterator = bufferState.get().iterator();
            if (iterator.hasNext()) {
                // 获取当前时间
                long currentTime = System.currentTimeMillis();
                Long lastProcessTime = lastProcessTimeState.value();

                // 确保至少间隔interval毫秒
                if (lastProcessTime == null || (currentTime - lastProcessTime) >= interval) {
                    String value = iterator.next();
                    // 从状态中移除处理的消息
                    List<String> newBuffer = new ArrayList<>();
                    while (iterator.hasNext()) {
                        newBuffer.add(iterator.next());
                    }
                    bufferState.update(newBuffer);

                    out.collect(value);
                    lastProcessTimeState.update(currentTime);
                    System.out.println("处理消息: " + currentTime);
                }
            }

            // 如果缓冲区还有数据，注册下一个定时器
            if (bufferState.get().iterator().hasNext()) {
                long nextTimer = System.currentTimeMillis() + interval;
                ctx.timerService().registerProcessingTimeTimer(nextTimer);
                nextTimerState.update(nextTimer);
                System.out.println("注册下一个限流定时器: " + nextTimer);
            } else {
                nextTimerState.clear();
            }
        }
    }

    // 时间戳解析方法
    private static long parseTimestamp(String timeStamp) {
        try {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS");
            LocalDateTime dateTime = LocalDateTime.parse(timeStamp, formatter);
            return dateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        } catch (Exception e) {
            System.err.println("时间戳解析错误: " + timeStamp + " - " + e.getMessage());
            return System.currentTimeMillis();
        }
    }

    // 轨迹处理窗口函数
    private static class TrajectoryProcessWindowFunction
            extends ProcessWindowFunction<VehicleDataPoint, VehicleTrajectory, String, TimeWindow> {

        @Override
        public void process(
                String plateNo,
                Context context,
                Iterable<VehicleDataPoint> points,
                Collector<VehicleTrajectory> out
        ) {
            List<VehicleDataPoint> sortedPoints = new ArrayList<>();
            for (VehicleDataPoint point : points) {
                sortedPoints.add(point);
            }
            sortedPoints.sort(Comparator.comparingLong(VehicleDataPoint::getTimestamp));

            if (sortedPoints.isEmpty()) {
                System.out.println("空轨迹窗口: " + plateNo);
                return;
            }

            // 创建轨迹对象
            VehicleTrajectory trajectory = new VehicleTrajectory();
            trajectory.setPlateNo(plateNo);
            trajectory.setVehicleType(sortedPoints.get(0).getVehicleType());
            trajectory.setStartTime(sortedPoints.get(0).getTimestamp());
            trajectory.setEndTime(sortedPoints.get(sortedPoints.size()-1).getTimestamp());
            trajectory.setTotalPoints(sortedPoints.size());

            double totalSpeed = 0;
            double lastLat = 0, lastLon = 0;
            boolean firstPoint = true;
            double totalDistance = 0;

            // 添加数据点到轨迹
            for (VehicleDataPoint point : sortedPoints) {
                Map<String, Object> dataPoint = new HashMap<>();
                dataPoint.put("timestamp", point.getTimestamp());
                dataPoint.put("longitude", point.getLongitude());
                dataPoint.put("latitude", point.getLatitude());
                dataPoint.put("speed", point.getSpeed());
                dataPoint.put("laneNo", point.getLaneNo());
                dataPoint.put("stakeId", point.getStakeId());
                dataPoint.put("vehicleWeight", point.getVehicleWeight());

                trajectory.getPoints().add(dataPoint);
                totalSpeed += point.getSpeed();

                // 计算大致距离
                if (!firstPoint) {
                    totalDistance += distance(lastLat, lastLon,
                            point.getLatitude(), point.getLongitude());
                } else {
                    firstPoint = false;
                }
                lastLat = point.getLatitude();
                lastLon = point.getLongitude();
            }

            trajectory.setAvgSpeed(totalSpeed / sortedPoints.size());
            trajectory.setTotalDistance(totalDistance);

            out.collect(trajectory);
        }
    }

    // Redis存储处理器
    private static class RedisStorageProcessor
            extends KeyedProcessFunction<String, Tuple2<String, VehicleTrajectory>, Void> {

        private transient JedisPool jedisPool;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            // 初始化Redis连接池
            JedisPoolConfig poolConfig = new JedisPoolConfig();
            poolConfig.setMaxTotal(200);
            poolConfig.setMaxIdle(32);
            poolConfig.setMinIdle(10);
            poolConfig.setMaxWaitMillis(100 * 1000);
            poolConfig.setBlockWhenExhausted(true);
            poolConfig.setTestOnBorrow(true);

            jedisPool = new JedisPool(poolConfig, REDIS_HOST, REDIS_PORT, 60000, REDIS_PASSWORD);
            System.out.println("Redis存储处理器初始化完成");
        }

        @Override
        public void processElement(
                Tuple2<String, VehicleTrajectory> value,
                Context ctx,
                Collector<Void> out
        ) throws Exception {
            String plateNo = value.f0;
            VehicleTrajectory trajectory = value.f1;

            Jedis jedis = null;
            try {
                jedis = jedisPool.getResource();
                String key = REDIS_KEY_PREFIX + plateNo;
                String trajectoryJson = JSON.toJSONString(trajectory);

                // 使用Redis列表存储轨迹（追加到现有轨迹）
                jedis.rpush(key, trajectoryJson);
                System.out.println("存储轨迹到Redis: key=" + key + ", 轨迹=" + trajectory);

                // 设置过期时间（1小时）
                jedis.expire(key, 3600);
                System.out.println("设置Redis键过期时间: key=" + key + ", 过期时间=3600秒");
            } finally {
                if (jedis != null) {
                    jedis.close();
                }
            }
        }

        @Override
        public void close() {
            if (jedisPool != null) {
                jedisPool.close();
                System.out.println("Redis存储处理器关闭");
            }
        }
    }

    // 定时触发器函数
    private static class TimerTriggerFunction
            extends KeyedProcessFunction<Integer, Long, Long> {

        private final long interval;
        private ValueState<Long> nextTimerState;

        public TimerTriggerFunction(long interval) {
            this.interval = interval;
        }

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<>("next-timer", Long.class);
            nextTimerState = getRuntimeContext().getState(descriptor);
            System.out.println("定时触发器初始化完成，间隔: " + interval + "ms");
        }

        @Override
        public void processElement(Long value, Context ctx, Collector<Long> out) throws Exception {
            if (nextTimerState.value() == null) {
                long nextTimer = System.currentTimeMillis() + interval;
                ctx.timerService().registerProcessingTimeTimer(nextTimer);
                nextTimerState.update(nextTimer);
                System.out.println("注册第一个定时器: " + nextTimer);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Long> out) throws Exception {
            System.out.println("定时器触发: " + timestamp);
            out.collect(timestamp);

            long nextTimer = timestamp + interval;
            ctx.timerService().registerProcessingTimeTimer(nextTimer);
            nextTimerState.update(nextTimer);
            System.out.println("注册下一个定时器: " + nextTimer);
        }
    }

    // Redis到HBase处理器（使用动态表）
    private static class RedisToHBaseProcessor
            extends ProcessFunction<Long, String> {

        private transient JedisPool jedisPool;
        private transient Connection hbaseConnection;
        private transient Admin hbaseAdmin;
        private final ReentrantLock tableLock = new ReentrantLock();
        private static final ConcurrentHashMap<String, Object> tableCreationLocks = new ConcurrentHashMap<>();

        // 动态表相关变量
        private transient String currentTableName;
        private transient LocalDateTime nextTableSwitchTime;
        private transient Table currentTable;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            // 初始化Redis连接池
            JedisPoolConfig redisPoolConfig = new JedisPoolConfig();
            redisPoolConfig.setMaxTotal(200);
            redisPoolConfig.setMaxIdle(32);
            redisPoolConfig.setMinIdle(10);
            redisPoolConfig.setMaxWaitMillis(100 * 1000);
            redisPoolConfig.setBlockWhenExhausted(true);
            redisPoolConfig.setTestOnBorrow(true);

            jedisPool = new JedisPool(redisPoolConfig, REDIS_HOST, REDIS_PORT, 60000, REDIS_PASSWORD);
            System.out.println("Redis连接池初始化完成");

            // 初始化HBase连接
            Configuration hbaseConfig = HBaseConfiguration.create();
            hbaseConfig.set("hbase.zookeeper.quorum", HBASE_ZOOKEEPER_QUORUM);
            hbaseConfig.set("hbase.zookeeper.property.clientPort", HBASE_ZOOKEEPER_PORT);
            hbaseConfig.set("zookeeper.znode.parent", "/hbase");

            try {
                hbaseConnection = ConnectionFactory.createConnection(hbaseConfig);
                hbaseAdmin = hbaseConnection.getAdmin();
                System.out.println("HBase连接初始化完成");
            } catch (IOException e) {
                System.err.println("HBase连接初始化失败: " + e.getMessage());
                throw new RuntimeException(e);
            }
        }

        @Override
        public void processElement(Long timestamp, Context ctx, Collector<String> out) throws Exception {
            System.out.println("处理定时触发事件: " + timestamp);

            Jedis jedis = null;
            try {
                jedis = jedisPool.getResource();
                // 扫描所有车辆轨迹键
                Set<String> keys = jedis.keys(REDIS_KEY_PREFIX + "*");
                System.out.println("从Redis读取轨迹数据: 键数量=" + keys.size());

                if (keys != null && !keys.isEmpty()) {
                    // 批量处理所有车辆轨迹
                    for (String key : keys) {
                        String plateNo = key.substring(REDIS_KEY_PREFIX.length());
                        System.out.println("处理车辆: " + plateNo);

                        // 获取所有轨迹片段
                        List<String> trajectoryJsons = jedis.lrange(key, 0, -1);
                        System.out.println("轨迹片段数量: " + trajectoryJsons.size());

                        // 合并轨迹片段
                        VehicleTrajectory mergedTrajectory = mergeTrajectories(trajectoryJsons);

                        // 存入HBase（使用动态表）
                        saveToDynamicHBase(mergedTrajectory);

                        // 清空Redis
                        jedis.del(key);
                        System.out.println("清空Redis键: key=" + key);
                    }
                    out.collect("成功处理 " + keys.size() + " 辆车的轨迹数据");
                } else {
                    System.out.println("Redis中没有找到轨迹数据");
                    out.collect("没有找到轨迹数据");
                }
            } finally {
                if (jedis != null) {
                    jedis.close();
                }
            }
        }

        private VehicleTrajectory mergeTrajectories(List<String> trajectoryJsons) {
            VehicleTrajectory merged = new VehicleTrajectory();
            long minStartTime = Long.MAX_VALUE;
            long maxEndTime = Long.MIN_VALUE;
            int totalPoints = 0;
            double totalSpeed = 0;
            double totalDistance = 0;

            for (String json : trajectoryJsons) {
                VehicleTrajectory trajectory = JSON.parseObject(json, VehicleTrajectory.class);

                if (merged.getPlateNo() == null) {
                    merged.setPlateNo(trajectory.getPlateNo());
                    merged.setVehicleType(trajectory.getVehicleType());
                }

                minStartTime = Math.min(minStartTime, trajectory.getStartTime());
                maxEndTime = Math.max(maxEndTime, trajectory.getEndTime());
                totalPoints += trajectory.getTotalPoints();
                totalSpeed += trajectory.getAvgSpeed() * trajectory.getTotalPoints();
                totalDistance += trajectory.getTotalDistance();

                merged.getPoints().addAll(trajectory.getPoints());
            }

            merged.setStartTime(minStartTime);
            merged.setEndTime(maxEndTime);
            merged.setTotalPoints(totalPoints);
            merged.setAvgSpeed(totalSpeed / totalPoints);
            merged.setTotalDistance(totalDistance);

            return merged;
        }

        // 使用动态表存储轨迹
        private void saveToDynamicHBase(VehicleTrajectory trajectory) throws IOException {
            // 根据轨迹开始时间确定表名
            long rowKeyTime = trajectory.getStartTime();
            LocalDateTime rowKeyDateTime = LocalDateTime.ofInstant(
                    Instant.ofEpochMilli(rowKeyTime), ZoneId.systemDefault()
            );

            // 切换表（如果必要）
            if (currentTableName == null || isTimeToSwitch(rowKeyTime)) {
                switchTable(rowKeyTime);
            }

            // 创建行键（车牌号 + 开始时间）
            String rowKey = trajectory.getPlateNo() + "_" + trajectory.getStartTime();
            Put put = new Put(Bytes.toBytes(rowKey));

            // 添加轨迹数据
            put.addColumn(Bytes.toBytes(HBASE_COLUMN_FAMILY),
                    Bytes.toBytes("plateNo"),
                    Bytes.toBytes(trajectory.getPlateNo()));

            put.addColumn(Bytes.toBytes(HBASE_COLUMN_FAMILY),
                    Bytes.toBytes("vehicleType"),
                    Bytes.toBytes(String.valueOf(trajectory.getVehicleType())));

            put.addColumn(Bytes.toBytes(HBASE_COLUMN_FAMILY),
                    Bytes.toBytes("startTime"),
                    Bytes.toBytes(String.valueOf(trajectory.getStartTime())));

            put.addColumn(Bytes.toBytes(HBASE_COLUMN_FAMILY),
                    Bytes.toBytes("endTime"),
                    Bytes.toBytes(String.valueOf(trajectory.getEndTime())));

            put.addColumn(Bytes.toBytes(HBASE_COLUMN_FAMILY),
                    Bytes.toBytes("totalPoints"),
                    Bytes.toBytes(String.valueOf(trajectory.getTotalPoints())));

            put.addColumn(Bytes.toBytes(HBASE_COLUMN_FAMILY),
                    Bytes.toBytes("avgSpeed"),
                    Bytes.toBytes(String.valueOf(trajectory.getAvgSpeed())));

            put.addColumn(Bytes.toBytes(HBASE_COLUMN_FAMILY),
                    Bytes.toBytes("totalDistance"),
                    Bytes.toBytes(String.valueOf(trajectory.getTotalDistance())));

            put.addColumn(Bytes.toBytes(HBASE_COLUMN_FAMILY),
                    Bytes.toBytes("trajectory"),
                    Bytes.toBytes(JSON.toJSONString(trajectory.getPoints())));

            // 保存到HBase
            currentTable.put(put);
            System.out.println("保存轨迹到HBase: table=" + currentTableName + ", rowKey=" + rowKey);
        }

        // 检查是否需要切换表
        private boolean isTimeToSwitch(long rowKeyTime) {
            LocalDateTime rowKeyDateTime = LocalDateTime.ofInstant(
                    Instant.ofEpochMilli(rowKeyTime), ZoneId.systemDefault()
            );
            return rowKeyDateTime.isAfter(nextTableSwitchTime);
        }

        // 切换到新表
        private void switchTable(long rowKeyTime) throws IOException {
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
        public void createTableIfNotExists(String tableName, String columnFamily) throws IOException {
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

        @Override
        public void close() throws Exception {
            if (jedisPool != null) {
                jedisPool.close();
            }
            if (hbaseAdmin != null) {
                hbaseAdmin.close();
            }
            if (hbaseConnection != null) {
                hbaseConnection.close();
            }
            if (currentTable != null) {
                currentTable.close();
            }
            System.out.println("Redis到HBase处理器关闭");
        }
    }

    // 计算两点间距离（简化版）
    private static double distance(double lat1, double lon1, double lat2, double lon2) {
        final int R = 6371; // 地球半径(km)
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dLat/2) * Math.sin(dLat/2) +
                Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) *
                        Math.sin(dLon/2) * Math.sin(dLon/2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
        return R * c; // 返回公里数
    }
}