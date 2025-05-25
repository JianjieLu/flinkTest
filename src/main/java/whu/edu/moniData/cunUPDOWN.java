package whu.edu.moniData;

import com.alibaba.fastjson2.JSONArray;
import javafx.util.Pair;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import whu.edu.ljj.flink.utils.myTools;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import whu.edu.ljj.flink.merge.Version1.Utils.PathPoint;

import lombok.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import static whu.edu.ljj.flink.merge.Version1.Utils.convertToTimestampMillis;
import static whu.edu.ljj.flink.xiaohanying.Utils.*;
import static whu.edu.moniData.Utils.totalOps.putLine;

public class cunUPDOWN {

    static Map<String, Integer> upcount = new ConcurrentHashMap<>();
    static Map<String, Integer> downcount = new ConcurrentHashMap<>();
    static Map<String, Integer> idTid = new ConcurrentHashMap<>();
    static Map<String, String> lastTime = new ConcurrentHashMap<>();
    static Map<String, Boolean> firstInput = new ConcurrentHashMap<>();
    static Map<String, String> bigIdToSmallId = new ConcurrentHashMap<>();
    static int ii1;
    static int ii2;
    static Map<Integer, Pair<Integer,Integer>> mmap=new ConcurrentHashMap<>();
    static Configuration conf = HBaseConfiguration.create();
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS");

    public static void main(String[] args) throws Exception {
        String i1 = args[0];
        String i2 = args[1];
        ii1 = Integer.parseInt(i1);
        ii2 = Integer.parseInt(i2);
        bigIdToSmallId.put("XG01","C7370151-2116-470A-8E26-5F878B3C9D78");
        idTid.put("C7370151-2116-470A-8E26-5F878B3C9D78", 8);
        firstInput.put("C7370151-2116-470A-8E26-5F878B3C9D78", true);
        conf.set("hbase.zookeeper.quorum", "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142,100.65.38.36,100.65.38.37,100.65.38.38");
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
        DataStream<Tuple3<String, Integer, Long>> stationLaneStream = unionStream
                .keyBy(json -> {
                    // 解析 JSON 获取 orgcode 作为 key
                    try {
                        JSONObject jsonObj = JSON.parseObject(json);
                        String bigOrgCode = jsonObj.getString("orgCode");
                        return bigIdToSmallId.getOrDefault(bigOrgCode, "unknown");
                    } catch (Exception e) {
                        return "parse_error";
                    }
                })
                .process(new KeyedProcessFunction<String, String, Tuple3<String, Integer, Long>>() {
                    private transient ValueState<Integer> upCountState;
                    private transient ValueState<Integer> downCountState;
                    private transient ValueState<String> lastTimeState;

                    @Override
                    public void open( org.apache.flink.configuration.Configuration parameters) {
                        // 初始化状态描述符
                        ValueStateDescriptor<Integer> upDesc =
                                new ValueStateDescriptor<>("upCount", Types.INT);
                        ValueStateDescriptor<Integer> downDesc =
                                new ValueStateDescriptor<>("downCount", Types.INT);
                        ValueStateDescriptor<String> timeDesc =
                                new ValueStateDescriptor<>("lastTime", Types.STRING);

                        upCountState = getRuntimeContext().getState(upDesc);
                        downCountState = getRuntimeContext().getState(downDesc);
                        lastTimeState = getRuntimeContext().getState(timeDesc);
                    }

                    @Override
                    public void processElement(String jsonString, Context ctx,
                                               Collector<Tuple3<String, Integer, Long>> out) throws Exception {
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
                            Integer targetId = idTid.get(orgcode);

                            // 初始化状态
                            if (lastTimeState.value() == null) {
                                upCountState.update(0);
                                downCountState.update(0);
                                lastTimeState.update(timeKey);
                            }
                             Map<Integer, Pair<Integer,Integer>> tempMap=new ConcurrentHashMap<>();

                            // 处理 targetList
                            JSONArray targetList = jsonObj.getJSONArray("targetList");
                            if (targetList != null) {
                                for (int i = 0; i < targetList.size(); i++) {
                                    JSONObject target = targetList.getJSONObject(i);
                                    Integer station = target.getInteger("station");
                                    if (station.equals(targetId)) {
                                        int lane = target.getIntValue("lane");
                                        Integer id = target.getInteger("id");
                                        tempMap.put(id, new Pair<>(station, lane));
                                    }
                                }
                            }

                            for (Map.Entry<Integer,Pair<Integer,Integer>> entry : tempMap.entrySet()) {
                                if(mmap.get(entry.getKey())==null){
                                    if (entry.getValue().getValue() % 2 == 0) {
                                        downCountState.update(downCountState.value() + 1);
                                    } else {
                                        upCountState.update(upCountState.value() + 1);
                                    }
                                }
                            }
                            mmap.putAll(tempMap);

                            // 时间窗口判断
                            String storedTimeKey = lastTimeState.value();
                            if (!timeKey.equals(storedTimeKey)) {
                                // 写入 HBase
                                long timestamp = parseTimestamp(thisTime);
                                long hourWindow = (timestamp / 3_600_000) * 3_600_000 - 3_600_000;

                                String rowKey = orgcode + "_" + hourWindow;
                                putLine(conf, "f1", "tab", rowKey, "downCount",
                                        String.valueOf(downCountState.value()));
                                putLine(conf, "f1", "tab", rowKey, "upCount",
                                        String.valueOf(upCountState.value()));

                                // 发送结果到下游（可选）
                                out.collect(new Tuple3<>(orgcode, upCountState.value(), hourWindow));

                                // 重置状态
                                upCountState.update(0);
                                downCountState.update(0);
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

        env.execute("Flink Traffic Statistics");
    }
}