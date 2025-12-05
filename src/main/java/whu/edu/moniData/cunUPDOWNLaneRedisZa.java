package whu.edu.moniData;

import com.alibaba.fastjson2.JSONArray;
import javafx.util.Pair;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class cunUPDOWNLaneRedisZa {

    static Map<String, Integer> idTid = new ConcurrentHashMap<>();
    static Map<String, Boolean> firstInput = new ConcurrentHashMap<>();
    static Map<String, String> bigIdToSmallId = new ConcurrentHashMap<>();
    static int ii1;
    static int ii2;
    static Map<Integer, Pair<Integer,Integer>> mmap=new ConcurrentHashMap<>();
    static Configuration conf = HBaseConfiguration.create();
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS");

    // HBase 表名和列族名常量
    private static final String TABLE_NAME = "tabl_lane";
    private static final String RAMP_TABLE_NAME = "tabl_ramp";
    private static final String COLUMN_FAMILY = "f1";

    // Redis 配置
    private static final String REDIS_HOST = "100.65.38.141";
    private static final int REDIS_PORT = 6380;
    private static final String REDIS_PASSWORD = "whdx123cgz666";
    private static final String REDIS_KEY_PREFIX = "traffic_stats:";
    private static final String REDIS_RAMP_KEY_PREFIX = "ramp_stats:";

    // 移植的表创建锁
    private static final ConcurrentHashMap<String, Object> tableCreationLocks = new ConcurrentHashMap<>();
    private static final ReentrantLock tableLock = new ReentrantLock();

    // 判断公交类型的方法
    private static boolean isBus(int vt) {
        return vt == 1 || vt == 3 || vt == 7 || vt == 15;
    }

    // 判断轨道类型的方法
    private static boolean isTrack(int vt) {
        return vt == 2 || vt == 10 || vt == 8 || vt == 11 || vt == 170 || vt == 171 || vt == 172 ||
                vt == 173 || vt == 174 || vt == 175 || vt == 176 || vt == 177;
    }

    // 判断客货车类型的方法
    private static int getVehicleClass(int originalType) {
        if ((originalType >= 1 && originalType <= 4) || originalType == 7 || (originalType >= 12 && originalType <= 16)) {
            return 0; // 客车
        }
        if (originalType == 8 || originalType == 10 || originalType == 11 ||
                (originalType >= 170 && originalType <= 177)) {
            return 1; // 货车
        }
        return -1;
    }

    // 提取匝道编号的方法
    private static String extractRampCode(String stakeId) {
        if (stakeId == null || !stakeId.contains("-")) {
            return null;
        }

        String[] parts = stakeId.split("-");
        if (parts.length < 2) {
            return null;
        }

        // 获取最后一个部分并提取字母
        String lastPart = parts[parts.length - 1];
        for (char c : lastPart.toCharArray()) {
            if (Character.isLetter(c)) {
                System.out.println(String.valueOf(c).toUpperCase());
                return String.valueOf(c).toUpperCase();
            }
        }

        return null;
    }

    public static void main(String[] args) throws Exception {
        String i1 = args[0];
        String i2 = args[1];
        ii1 = Integer.parseInt(i1);
        ii2 = Integer.parseInt(i2);
        bigIdToSmallId.put("XG01","C7370151-2116-470A-8E26-5F878B3C9D78");
        idTid.put("C7370151-2116-470A-8E26-5F878B3C9D78", 8);
        firstInput.put("C7370151-2116-470A-8E26-5F878B3C9D78", true);

        // 配置 HBase
        conf.set("hbase.zookeeper.quorum", "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142,10.48.53.80");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        // Kafka配置 - 主数据流
        String brokers = "100.65.38.40:9092";
        String groupId = "flink-group";
        List<String> topics = Arrays.asList("e1_data_XG01");

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(topics.get(0))
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setProperty("auto.offset.commit", "true")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> unionStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source 1");

        for (int i = 1; i < topics.size(); i++) {
            KafkaSource<String> source = KafkaSource.<String>builder()
                    .setBootstrapServers(brokers)
                    .setTopics(topics.get(i))
                    .setGroupId(groupId)
                    .setStartingOffsets(OffsetsInitializer.latest())
                    .setProperty("auto.offset.commit", "true")
                    .setValueOnlyDeserializer(new SimpleStringSchema())
                    .build();

            DataStream<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source " + (i + 1));
            unionStream = unionStream.union(stream);
        }

        // 使用Tuple7: <rowKey, lane, busCount, trackCount, totalCount, totalSpeed, vehicleCount>
        DataStream<Tuple7<String, Integer, Integer, Integer, Integer, Double, Integer>> statsStream = unionStream
                .keyBy(json -> {
                    try {
                        JSONObject jsonObj = JSON.parseObject(json);
                        String bigOrgCode = jsonObj.getString("orgCode");
                        return bigIdToSmallId.getOrDefault(bigOrgCode, "unknown");
                    } catch (Exception e) {
                        return "parse_error";
                    }
                })
                .process(new KeyedProcessFunction<String, String, Tuple7<String, Integer, Integer, Integer, Integer, Double, Integer>>() {
                    // 按车道存储统计信息
                    private transient MapState<Integer, Tuple5<Integer, Integer, Integer, Double, Integer>> laneStatsState; // 车道号 -> (busCount, trackCount, totalCount, totalSpeed, vehicleCount)
                    private transient ValueState<String> lastTimeState;
                    private transient ValueState<Integer> lastMinuteState;
                    private transient MapState<Integer, Boolean> processedIdsState; // 用于车辆去重

                    @Override
                    public void open(org.apache.flink.configuration.Configuration parameters) {
                        // 初始化状态
                        MapStateDescriptor<Integer, Tuple5<Integer, Integer, Integer, Double, Integer>> laneStatsDesc =
                                new MapStateDescriptor<>("laneStats", Types.INT,
                                        Types.TUPLE(Types.INT, Types.INT, Types.INT, Types.DOUBLE, Types.INT));

                        ValueStateDescriptor<String> timeDesc =
                                new ValueStateDescriptor<>("lastTime", Types.STRING);

                        ValueStateDescriptor<Integer> minuteDesc =
                                new ValueStateDescriptor<>("lastMinute", Types.INT);

                        // 使用 MapState 替代 SetState
                        MapStateDescriptor<Integer, Boolean> processedIdsDesc =
                                new MapStateDescriptor<>("processedIds", Types.INT, Types.BOOLEAN);

                        laneStatsState = getRuntimeContext().getMapState(laneStatsDesc);
                        lastTimeState = getRuntimeContext().getState(timeDesc);
                        lastMinuteState = getRuntimeContext().getState(minuteDesc);
                        processedIdsState = getRuntimeContext().getMapState(processedIdsDesc);
                    }

                    @Override
                    public void processElement(String jsonString, Context ctx,
                                               Collector<Tuple7<String, Integer, Integer, Integer, Integer, Double, Integer>> out) throws Exception {
                        try {
                            JSONObject jsonObj = JSON.parseObject(jsonString);
                            String bigOrgCode = jsonObj.getString("orgCode");
                            String orgcode = bigIdToSmallId.getOrDefault(bigOrgCode, "unknown");

                            if ("unknown".equals(orgcode)) {
                                System.err.println("Unknown orgcode: " + bigOrgCode);
                                return;
                            }

                            String thisTime = jsonObj.getString("globalTime");
                            String timeKey = thisTime.substring(ii1, ii2);
                            int myKey = Integer.parseInt(thisTime.substring(14, 16));
                            Integer targetId = idTid.get(orgcode);

                            // 初始化状态
                            if (lastTimeState.value() == null) {
                                lastTimeState.update(timeKey);
                                lastMinuteState.update(myKey);
                                laneStatsState.clear();
                                processedIdsState.clear();
                            }

                            Map<Integer, Pair<Integer,Integer>> tempMap = new ConcurrentHashMap<>();
                            Map<Integer, Integer> originalTypeMap = new ConcurrentHashMap<>();
                            Map<Integer, Double> speedMap = new ConcurrentHashMap<>(); // 存储每辆车的速度

                            // 处理 targetList
                            JSONArray targetList = jsonObj.getJSONArray("targetList");
                            if (targetList != null) {
                                for (int i = 0; i < targetList.size(); i++) {
                                    JSONObject target = targetList.getJSONObject(i);
                                    Integer station = target.getInteger("station");
                                    int lane = target.getIntValue("lane");
                                    Integer id = target.getInteger("id");
                                    Integer originalType = target.getInteger("carType");
                                    double speed = target.getDoubleValue("speed");

                                    if (station.equals(targetId)) {
                                        // 检查车辆是否已处理
                                        if (!processedIdsState.contains(id)) {
                                            tempMap.put(id, new Pair<>(station, lane));
                                            originalTypeMap.put(id, originalType);
                                            speedMap.put(id, speed); // 存储速度
                                            processedIdsState.put(id, true);
                                        }
                                    }
                                }
                            }

                            // 更新车道统计
                            for (Map.Entry<Integer, Pair<Integer,Integer>> entry : tempMap.entrySet()) {
                                int lane = entry.getValue().getValue();
                                int vehicleType = originalTypeMap.get(entry.getKey());
                                double speed = speedMap.get(entry.getKey());

                                // 只处理公交和轨道车辆
                                if (isBus(vehicleType) || isTrack(vehicleType)) {
                                    // 获取当前车道统计
                                    Tuple5<Integer, Integer, Integer, Double, Integer> currentStats = laneStatsState.get(lane);
                                    if (currentStats == null) {
                                        currentStats = Tuple5.of(0, 0, 0, 0.0, 0);
                                    }

                                    // 更新统计
                                    int busCount = currentStats.f0;
                                    int trackCount = currentStats.f1;
                                    int totalCount = currentStats.f2;
                                    double totalSpeed = currentStats.f3;
                                    int vehicleCount = currentStats.f4;

                                    if (isBus(vehicleType)) {
                                        busCount++;
                                        totalCount++;
                                    } else if (isTrack(vehicleType)) {
                                        trackCount++;
                                        totalCount++;
                                    }

                                    // 累加速度并增加车辆计数
                                    totalSpeed += speed;
                                    vehicleCount++;

                                    laneStatsState.put(lane, Tuple5.of(busCount, trackCount, totalCount, totalSpeed, vehicleCount));
                                }
                            }

                            long timestamp = parseTimestamp(thisTime);
                            long hourWindow = (timestamp / 3_600_000) * 3_600_000;
                            String baseRowKey = orgcode + "_" + hourWindow;

                            // 每分钟发射统计结果
                            int storedMinuteKey = lastMinuteState.value();
                            if (storedMinuteKey != myKey) {
                                System.out.println("storedMinuteKey:" + storedMinuteKey + "  myKey:" + myKey);

                                // 遍历所有车道，发射统计结果
                                for (Integer lane : laneStatsState.keys()) {
                                    Tuple5<Integer, Integer, Integer, Double, Integer> stats = laneStatsState.get(lane);
                                    String rowKey = baseRowKey + "_" + lane;

                                    out.collect(Tuple7.of(
                                            rowKey,
                                            lane,
                                            stats.f0,  // busCount
                                            stats.f1,  // trackCount
                                            stats.f2,  // totalCount
                                            stats.f3,  // totalSpeed
                                            stats.f4   // vehicleCount
                                    ));
                                }

                                lastMinuteState.update(myKey);
                            }

                            // 每小时清空
                            String storedTimeKey = lastTimeState.value();
                            if (!timeKey.equals(storedTimeKey)) {
                                laneStatsState.clear();
                                processedIdsState.clear();
                                lastTimeState.update(timeKey);
                            }
                        } catch (Exception e) {
                            System.err.println("处理数据异常: " + e.getMessage());
                            e.printStackTrace();
                        }
                    }

                    private long parseTimestamp(String timeString) throws DateTimeParseException {
                        return LocalDateTime.parse(timeString, TIME_FORMATTER)
                                .atZone(ZoneId.systemDefault())
                                .toInstant()
                                .toEpochMilli();
                    }
                });

        // 添加HBase和Redis两个Sink
        statsStream.addSink(new HBaseStatsSink());
        statsStream.addSink(new RedisStatsSink());

        // 创建匝道数据流处理
        KafkaSource<String> rampKafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics("MergedRampPathData")
                .setGroupId(groupId + "_ramp")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setProperty("auto.offset.commit", "true")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> rampStream = env.fromSource(rampKafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source Ramp");

        // 处理匝道数据流
        DataStream<Tuple7<String, String, Integer, Integer, Integer, Double, Integer>> rampStatsStream = rampStream
                .flatMap(new FlatMapFunction<String, Tuple4<String, Integer, Double, Long>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple4<String, Integer, Double, Long>> out) throws Exception {
                        try {
                            JSONObject jsonObj = JSON.parseObject(value);
                            String timeStamp = jsonObj.getString("timeStamp");
                            long timestamp = parseTimestamp(timeStamp);

                            JSONArray pathList = jsonObj.getJSONArray("pathList");
                            if (pathList != null) {
                                for (int i = 0; i < pathList.size(); i++) {
                                    JSONObject vehicle = pathList.getJSONObject(i);
                                    String stakeId = vehicle.getString("stakeId");
                                    String rampCode = extractRampCode(stakeId);

                                    if (rampCode != null && (rampCode.equals("A") || rampCode.equals("B") ||
                                            rampCode.equals("C") || rampCode.equals("D"))) {
                                        int originalType = vehicle.getIntValue("originalType");
                                        int vehicleClass = getVehicleClass(originalType);
                                        double speed = vehicle.getDoubleValue("speed");

                                        if (vehicleClass != -1) { // 只处理客车和货车
                                            out.collect(Tuple4.of(rampCode, vehicleClass, speed, timestamp));
                                        }
                                    }
                                }
                            }
                        } catch (Exception e) {
                            System.err.println("处理匝道数据异常: " + e.getMessage());
                            e.printStackTrace();
                        }
                    }

                    private long parseTimestamp(String timeString) throws DateTimeParseException {
                        return LocalDateTime.parse(timeString, TIME_FORMATTER)
                                .atZone(ZoneId.systemDefault())
                                .toInstant()
                                .toEpochMilli();
                    }
                })
                .keyBy(tuple -> tuple.f0) // 按匝道编号分组
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new AggregateFunction<Tuple4<String, Integer, Double, Long>,
                        Tuple5<Integer, Integer, Double, Integer, Long>, // 累加器类型: (客车数, 货车数, 总速度, 车辆数, 最后时间戳)
                        Tuple7<String, String, Integer, Integer, Integer, Double, Integer>>() {

                    @Override
                    public Tuple5<Integer, Integer, Double, Integer, Long> createAccumulator() {
                        return Tuple5.of(0, 0, 0.0, 0, 0L);
                    }

                    @Override
                    public Tuple5<Integer, Integer, Double, Integer, Long> add(
                            Tuple4<String, Integer, Double, Long> value,
                            Tuple5<Integer, Integer, Double, Integer, Long> accumulator) {

                        int carCount = accumulator.f0;
                        int trackCount = accumulator.f1;
                        double totalSpeed = accumulator.f2;
                        int vehicleCount = accumulator.f3;

                        if (value.f1 == 0) { // 客车
                            carCount++;
                        } else if (value.f1 == 1) { // 货车
                            trackCount++;
                        }

                        totalSpeed += value.f2;
                        vehicleCount++;

                        return Tuple5.of(carCount, trackCount, totalSpeed, vehicleCount, value.f3);
                    }

                    // 添加的 merge() 方法
                    @Override
                    public Tuple5<Integer, Integer, Double, Integer, Long> merge(
                            Tuple5<Integer, Integer, Double, Integer, Long> a,
                            Tuple5<Integer, Integer, Double, Integer, Long> b) {

                        return Tuple5.of(
                                a.f0 + b.f0,       // 客车数
                                a.f1 + b.f1,       // 货车数
                                a.f2 + b.f2,       // 总速度
                                a.f3 + b.f3,       // 车辆数
                                Math.max(a.f4, b.f4) // 时间戳取最大值
                        );
                    }

                    @Override
                    public Tuple7<String, String, Integer, Integer, Integer, Double, Integer> getResult(
                            Tuple5<Integer, Integer, Double, Integer, Long> accumulator) {

                        int carCount = accumulator.f0;
                        int trackCount = accumulator.f1;
                        double totalSpeed = accumulator.f2;
                        int vehicleCount = accumulator.f3;
                        long timestamp = accumulator.f4;

                        // 计算窗口开始时间
                        long windowStart = (timestamp / 3_600_000) * 3_600_000;
                        String rampCode = ""; // 这里无法获取匝道编号，需要在后面补充

                        return Tuple7.of("", rampCode, carCount, trackCount, carCount + trackCount, totalSpeed, vehicleCount);
                    }
                }, new ProcessWindowFunction<Tuple7<String, String, Integer, Integer, Integer, Double, Integer>,
                        Tuple7<String, String, Integer, Integer, Integer, Double, Integer>, String, TimeWindow>() {

                    @Override
                    public void process(String rampCode, Context context,
                                        Iterable<Tuple7<String, String, Integer, Integer, Integer, Double, Integer>> elements,
                                        Collector<Tuple7<String, String, Integer, Integer, Integer, Double, Integer>> out) throws Exception {

                        Tuple7<String, String, Integer, Integer, Integer, Double, Integer> result = elements.iterator().next();
                        long windowStart = context.window().getStart();

                        // 创建rowKey: 匝道编号_时间戳
                        String rowKey = rampCode + "_" + windowStart;

                        out.collect(Tuple7.of(rowKey, rampCode, result.f2, result.f3, result.f4, result.f5, result.f6));
                    }
                });

        // 添加匝道数据的Sink
        rampStatsStream.addSink(new RampHBaseStatsSink());
        rampStatsStream.addSink(new RampRedisStatsSink());

        env.execute("Flink Traffic Statistics with Avg Speed and Ramp Data");
    }

    private static class HBaseStatsSink extends RichSinkFunction<Tuple7<String, Integer, Integer, Integer, Integer, Double, Integer>> {
        private Connection connection;
        private Table table;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            super.open(parameters);
            connection = ConnectionFactory.createConnection(conf);
            createTableIfNotExists(TABLE_NAME, COLUMN_FAMILY, connection);
            table = connection.getTable(TableName.valueOf(TABLE_NAME));
        }

        @Override
        public void invoke(Tuple7<String, Integer, Integer, Integer, Integer, Double, Integer> stats, Context context) throws Exception {
            String rowKey = stats.f0;
            int lane = stats.f1;
            int busCount = stats.f2;
            int trackCount = stats.f3;
            int totalCount = stats.f4;
            double totalSpeed = stats.f5;
            int vehicleCount = stats.f6;

            // 计算平均速度
            double avgSpeed = vehicleCount > 0 ? totalSpeed / vehicleCount : 0.0;
            avgSpeed= (double) Math.round(avgSpeed * 100) /100;

            try {
                Put put = new Put(Bytes.toBytes(rowKey));

                // 添加车道信息
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("lane"), Bytes.toBytes(String.valueOf(lane)));

                // 添加统计信息
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("busCount"), Bytes.toBytes(String.valueOf(busCount)));
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("trackCount"), Bytes.toBytes(String.valueOf(trackCount)));
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("totalCount"), Bytes.toBytes(String.valueOf(totalCount)));
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("totalSpeed"), Bytes.toBytes(String.valueOf(totalSpeed)));
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("vehicleCount"), Bytes.toBytes(String.valueOf(vehicleCount)));
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("avgSpeed"), Bytes.toBytes(String.valueOf(avgSpeed)));

                table.put(put);
                System.out.println("成功写入HBase: " + rowKey);
            } catch (Exception e) {
                System.err.println("写入 HBase 失败: " + e.getMessage());
                e.printStackTrace();
            }
        }

        @Override
        public void close() throws Exception {
            if (table != null) table.close();
            if (connection != null) connection.close();
            super.close();
        }
    }

    private static class RedisStatsSink extends RichSinkFunction<Tuple7<String, Integer, Integer, Integer, Integer, Double, Integer>> {
        private transient JedisPool jedisPool;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            super.open(parameters);
            // 初始化Redis连接池
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

        @Override
        public void invoke(Tuple7<String, Integer, Integer, Integer, Integer, Double, Integer> stats, Context context) throws Exception {
            String rowKey = stats.f0;
            int lane = stats.f1;
            int busCount = stats.f2;
            int trackCount = stats.f3;
            int totalCount = stats.f4;
            double totalSpeed = stats.f5;
            int vehicleCount = stats.f6;

            Jedis jedis = null;
            try {
                jedis = jedisPool.getResource();

                // 解析基础键（不含车道号）
                String[] parts = rowKey.split("_");
                String baseKey = parts[0] + "_" + parts[1];

                // 创建车道统计对象
                JSONObject laneStats = new JSONObject();
                laneStats.put("busCount", busCount);
                laneStats.put("trackCount", trackCount);
                laneStats.put("totalCount", totalCount);
                laneStats.put("totalSpeed", totalSpeed); // 存储总速度
                laneStats.put("vehicleCount", vehicleCount); // 存储车辆数

                // 使用Hash结构存储数据
                String redisKey = REDIS_KEY_PREFIX + baseKey;

                // 设置车道统计
                jedis.hset(redisKey, "lane_" + lane, laneStats.toJSONString());

                // 设置过期时间（24小时）
                jedis.expire(redisKey, 24 * 60 * 60);

                System.out.println("成功写入Redis: " + redisKey + " 车道: " + lane);
            } catch (Exception e) {
                System.err.println("写入Redis失败: " + e.getMessage());
                e.printStackTrace();
            } finally {
                if (jedis != null) {
                    jedis.close();
                }
            }
        }

        @Override
        public void close() throws Exception {
            if (jedisPool != null) {
                jedisPool.close();
                System.out.println("Redis连接池已关闭");
            }
            super.close();
        }
    }

    // 匝道数据HBase Sink
    private static class RampHBaseStatsSink extends RichSinkFunction<Tuple7<String, String, Integer, Integer, Integer, Double, Integer>> {
        private Connection connection;
        private Table table;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            super.open(parameters);
            System.out.println("[RampHBaseSink] 正在初始化HBase连接...");
            connection = ConnectionFactory.createConnection(conf);
            createTableIfNotExists(RAMP_TABLE_NAME, COLUMN_FAMILY, connection);
            table = connection.getTable(TableName.valueOf(RAMP_TABLE_NAME));
            System.out.println("[RampHBaseSink] HBase连接初始化完成");
        }

        @Override
        public void invoke(Tuple7<String, String, Integer, Integer, Integer, Double, Integer> stats, Context context) throws Exception {
            System.out.println("[RampHBaseSink] 接收到匝道统计数据: " + stats);

            String rowKey = stats.f0;
            String rampCode = stats.f1;
            int carCount = stats.f2;
            int trackCount = stats.f3;
            int totalCount = stats.f4;
            double totalSpeed = stats.f5;
            int vehicleCount = stats.f6;

            // 计算平均速度
            double avgSpeed = vehicleCount > 0 ? totalSpeed / vehicleCount : 0.0;
            avgSpeed = (double) Math.round(avgSpeed * 100) / 100;
            System.out.println("[RampHBaseSink] 计算平均速度: " + avgSpeed);

            try {
                System.out.println("[RampHBaseSink] 准备写入HBase，RowKey: " + rowKey);
                Put put = new Put(Bytes.toBytes(rowKey));

                // 添加匝道信息
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("rampCode"), Bytes.toBytes(rampCode));

                // 添加统计信息
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("carCount"), Bytes.toBytes(String.valueOf(carCount)));
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("trackCount"), Bytes.toBytes(String.valueOf(trackCount)));
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("totalCount"), Bytes.toBytes(String.valueOf(totalCount)));
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("totalSpeed"), Bytes.toBytes(String.valueOf(totalSpeed)));
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("vehicleCount"), Bytes.toBytes(String.valueOf(vehicleCount)));
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("avgSpeed"), Bytes.toBytes(String.valueOf(avgSpeed)));

                System.out.println("[RampHBaseSink] 执行HBase写入操作...");
                table.put(put);
                System.out.println("[RampHBaseSink] 成功写入匝道数据到HBase: " + rowKey);
                System.out.println("[RampHBaseSink] 写入数据详情: " +
                        "匝道=" + rampCode +
                        ", 客车=" + carCount +
                        ", 货车=" + trackCount +
                        ", 总数=" + totalCount +
                        ", 平均速度=" + avgSpeed);
            } catch (Exception e) {
                System.err.println("[RampHBaseSink] 写入匝道数据到 HBase 失败: " + e.getMessage());
                e.printStackTrace();
            }
        }

        @Override
        public void close() throws Exception {
            System.out.println("[RampHBaseSink] 关闭HBase连接...");
            if (table != null) table.close();
            if (connection != null) connection.close();
            super.close();
        }
    }

    // 匝道数据Redis Sink
    private static class RampRedisStatsSink extends RichSinkFunction<Tuple7<String, String, Integer, Integer, Integer, Double, Integer>> {
        private transient JedisPool jedisPool;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            super.open(parameters);
            // 初始化Redis连接池
            JedisPoolConfig poolConfig = new JedisPoolConfig();
            poolConfig.setMaxTotal(200);
            poolConfig.setMaxIdle(32);
            poolConfig.setMinIdle(10);
            poolConfig.setMaxWaitMillis(100 * 1000);
            poolConfig.setBlockWhenExhausted(true);
            poolConfig.setTestOnBorrow(true);

            jedisPool = new JedisPool(poolConfig, REDIS_HOST, REDIS_PORT, 60000, REDIS_PASSWORD);
            System.out.println("Redis连接池初始化成功(匝道)");
        }

        @Override
        public void invoke(Tuple7<String, String, Integer, Integer, Integer, Double, Integer> stats, Context context) throws Exception {
            String rowKey = stats.f0;
            String rampCode = stats.f1;
            int carCount = stats.f2;
            int trackCount = stats.f3;
            int totalCount = stats.f4;
            double totalSpeed = stats.f5;
            int vehicleCount = stats.f6;

            Jedis jedis = null;
            try {
                jedis = jedisPool.getResource();

                // 创建匝道统计对象
                JSONObject rampStats = new JSONObject();
                rampStats.put("rampCode", rampCode);
                rampStats.put("carCount", carCount);
                rampStats.put("trackCount", trackCount);
                rampStats.put("totalCount", totalCount);
                rampStats.put("totalSpeed", totalSpeed);
                rampStats.put("vehicleCount", vehicleCount);

                // 使用Hash结构存储数据
                String redisKey = REDIS_RAMP_KEY_PREFIX + rowKey;

                // 设置匝道统计
                jedis.hset(redisKey, "stats", rampStats.toJSONString());

                // 设置过期时间（24小时）
                jedis.expire(redisKey, 24 * 60 * 60);

                System.out.println("成功写入匝道数据到Redis: " + redisKey);
            } catch (Exception e) {
                System.err.println("写入匝道数据到Redis失败: " + e.getMessage());
                e.printStackTrace();
            } finally {
                if (jedis != null) {
                    jedis.close();
                }
            }
        }

        @Override
        public void close() throws Exception {
            if (jedisPool != null) {
                jedisPool.close();
                System.out.println("Redis连接池已关闭(匝道)");
            }
            super.close();
        }
    }

    private static void createTableIfNotExists(String tableName, String columnFamily, Connection connection) {
        tableLock.lock();
        try (Admin admin = connection.getAdmin()) {
            TableName hbaseTableName = TableName.valueOf(tableName);

            Object lock = tableCreationLocks.computeIfAbsent(tableName, k -> new Object());

            synchronized (lock) {
                if (!admin.tableExists(hbaseTableName)) {
                    HTableDescriptor tableDescriptor = new HTableDescriptor(hbaseTableName);
                    tableDescriptor.addFamily(new HColumnDescriptor(columnFamily));
                    try {
                        admin.createTable(tableDescriptor);
                        System.out.println("Table created: " + tableName);
                    } catch (TableExistsException e) {
                        // 处理表已存在但未检测到的情况
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