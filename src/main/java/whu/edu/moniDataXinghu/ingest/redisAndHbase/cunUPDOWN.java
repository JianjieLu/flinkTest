package whu.edu.moniDataXinghu.ingest.redisAndHbase;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import javafx.util.Pair;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class cunUPDOWN {

    static Map<String, Integer> idTid = new ConcurrentHashMap<>();
    static Map<String, Boolean> firstInput = new ConcurrentHashMap<>();
    static Map<String, String> bigIdToSmallId = new ConcurrentHashMap<>();
    static int ii1;
    static int ii2;
    static Map<Integer, Pair<Integer,Integer>> mmap=new ConcurrentHashMap<>();
    static Configuration conf = HBaseConfiguration.create();
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS");

    // HBase 表名和列族名常量
    private static final String TABLE_NAME = "tabl";
    private static final String COLUMN_FAMILY = "f1";

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

    public static void main(String[] args) throws Exception {
        String i1 = args[0];
        String i2 = args[1];
        ii1 = Integer.parseInt(i1);
        ii2 = Integer.parseInt(i2);
        bigIdToSmallId.put("XG01","C7370151-2116-470A-8E26-5F878B3C9D78");
        idTid.put("C7370151-2116-470A-8E26-5F878B3C9D78", 8);
        firstInput.put("C7370151-2116-470A-8E26-5F878B3C9D78", true);

        // 配置 HBase
        conf.set("hbase.zookeeper.quorum", "192.168.0.5,192.168.0.7,192.168.0.8:,192.168.0.9,192.168.0.11,192.168.0.12");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        // Kafka配置
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

        DataStream<Tuple5<String, Integer, Integer, Integer, Integer>> statsStream = unionStream
                .keyBy(json -> {
                    try {
                        JSONObject jsonObj = JSON.parseObject(json);
                        String bigOrgCode = jsonObj.getString("orgCode");
                        return bigIdToSmallId.getOrDefault(bigOrgCode, "unknown");
                    } catch (Exception e) {
                        return "parse_error";
                    }
                })
                .process(new KeyedProcessFunction<String, String, Tuple5<String, Integer, Integer, Integer, Integer>>() {
                    private transient ValueState<Integer> upBusCountState;
                    private transient ValueState<Integer> upTrackCountState;
                    private transient ValueState<Integer> downBusCountState;
                    private transient ValueState<Integer> downTrackCountState;
                    private transient ValueState<String> lastTimeState;
                    private transient ValueState<Integer> lastMinuteState;

                    @Override
                    public void open( org.apache.flink.configuration.Configuration parameters) {
                        ValueStateDescriptor<Integer> upBusDesc =
                                new ValueStateDescriptor<>("upBusCount", Types.INT);
                        ValueStateDescriptor<Integer> upTrackDesc =
                                new ValueStateDescriptor<>("upTrackCount", Types.INT);
                        ValueStateDescriptor<Integer> downBusDesc =
                                new ValueStateDescriptor<>("downBusCount", Types.INT);
                        ValueStateDescriptor<Integer> downTrackDesc =
                                new ValueStateDescriptor<>("downTrackCount", Types.INT);
                        ValueStateDescriptor<String> timeDesc =
                                new ValueStateDescriptor<>("lastTime", Types.STRING);
                        ValueStateDescriptor<Integer> minuteDesc =
                                new ValueStateDescriptor<>("lastMinute", Types.INT);

                        upBusCountState = getRuntimeContext().getState(upBusDesc);
                        upTrackCountState = getRuntimeContext().getState(upTrackDesc);
                        downBusCountState = getRuntimeContext().getState(downBusDesc);
                        downTrackCountState = getRuntimeContext().getState(downTrackDesc);
                        lastTimeState = getRuntimeContext().getState(timeDesc);
                        lastMinuteState = getRuntimeContext().getState(minuteDesc);
                    }

                    @Override
                    public void processElement(String jsonString, Context ctx,
                                               Collector<Tuple5<String, Integer, Integer, Integer, Integer>> out) throws Exception {
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
                                upBusCountState.update(0);
                                upTrackCountState.update(0);
                                downBusCountState.update(0);
                                downTrackCountState.update(0);
                                lastTimeState.update(timeKey);
                                lastMinuteState.update(myKey);
                            }

                            Map<Integer, Pair<Integer,Integer>> tempMap = new ConcurrentHashMap<>();
                            Map<Integer, Integer> originalTypeMap = new ConcurrentHashMap<>();

                            // 处理 targetList
                            JSONArray targetList = jsonObj.getJSONArray("targetList");
                            if (targetList != null) {
                                for (int i = 0; i < targetList.size(); i++) {
                                    JSONObject target = targetList.getJSONObject(i);
                                    Integer station = target.getInteger("station");
                                    int lane = target.getIntValue("lane");
                                    Integer id = target.getInteger("id");
                                    Integer originalType = target.getInteger("carType");
                                    System.out.println("station:"+station+" lane:"+lane+" id:"+id+" originalType:"+originalType);
                                    if (station.equals(targetId)) {
                                        System.out.println("合理！");
                                        tempMap.put(id, new Pair<>(station, lane));
                                        originalTypeMap.put(id, originalType);
                                    }
                                }
                            }

                            for (Map.Entry<Integer,Pair<Integer,Integer>> entry : tempMap.entrySet()) {
                                if(mmap.get(entry.getKey()) == null){
                                    Integer originalType = originalTypeMap.get(entry.getKey());

                                    // 根据车道和车辆类型更新计数
                                    if (entry.getValue().getValue() % 2 == 0) { // 下行（车道号为偶数）
                                        if (isBus(originalType)) { // 客车
                                            downBusCountState.update(downBusCountState.value() + 1);
                                        } else if (isTrack(originalType)) { // 货车
                                            downTrackCountState.update(downTrackCountState.value() + 1);
                                        }
                                    } else { // 上行（车道号为奇数）
                                        if (isBus(originalType)) { // 客车
                                            upBusCountState.update(upBusCountState.value() + 1);
                                        } else if (isTrack(originalType)) { // 货车
                                            upTrackCountState.update(upTrackCountState.value() + 1);
                                        }
                                    }
                                }
                            }
                            mmap.putAll(tempMap);

                            long timestamp = parseTimestamp(thisTime);
                            long hourWindow = (timestamp / 3_600_000) * 3_600_000;
                            String rowKey = orgcode + "_" + hourWindow;

                            // 每分钟发射统计结果
                            int storedMinuteKey = lastMinuteState.value();
                            if(storedMinuteKey != myKey){
                                System.out.println("storedMinuteKey:"+storedMinuteKey+"  myKey:"+myKey);

                                // 发射统计数据到下游
                                out.collect(Tuple5.of(
                                        rowKey,
                                        downBusCountState.value(),   // 下行公交
                                        downTrackCountState.value(), // 下行轨道
                                        upBusCountState.value(),    // 上行公交
                                        upTrackCountState.value()   // 上行轨道
                                ));

                                lastMinuteState.update(myKey);
                            }

                            // 每小时清空
                            String storedTimeKey = lastTimeState.value();
                            if (!timeKey.equals(storedTimeKey)) {
                                upBusCountState.update(0);
                                upTrackCountState.update(0);
                                downBusCountState.update(0);
                                downTrackCountState.update(0);
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

        statsStream.addSink(new HBaseStatsSink());

        env.execute("Flink Traffic Statistics");
    }

    private static class HBaseStatsSink extends RichSinkFunction<Tuple5<String, Integer, Integer, Integer, Integer>> {
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
        public void invoke(Tuple5<String, Integer, Integer, Integer, Integer> stats, Context context) throws Exception {
            String rowKey = stats.f0;
            int downBus = stats.f1;
            int downTrack = stats.f2;
            int upBus = stats.f3;
            int upTrack = stats.f4;

            try {
                Put put = new Put(Bytes.toBytes(rowKey));

                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("downBus"), Bytes.toBytes(String.valueOf(downBus)));
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("downTrack"), Bytes.toBytes(String.valueOf(downTrack)));
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("upBus"), Bytes.toBytes(String.valueOf(upBus)));
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("upTrack"), Bytes.toBytes(String.valueOf(upTrack)));
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("upCount"), Bytes.toBytes(String.valueOf(upTrack+upBus)));
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("downCount"), Bytes.toBytes(String.valueOf(downBus+downTrack)));

                table.put(put);
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