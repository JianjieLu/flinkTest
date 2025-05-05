package whu.edu.ljj.ago.oks;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.*;

public class DemoTraje {
    public static void main(String[] args) throws Exception {
        String topic = args[0]; // Kafka topic
        long timesplit = Long.parseLong(args[1]); // Kafka topic

        // 设置 Flink 流执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 配置 KafkaSource
        String bootstrapServers = "192.168.0.5:9092"; // Kafka集群地址
        String groupId = "flink-group"; // 消费者组ID

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest()) // 从最早的偏移量开始
                .setValueOnlyDeserializer(new SimpleStringSchema()) // 反序列化消息为String
                .build();

        // 从 KafkaSource 创建数据流
        DataStreamSource<String> kafkaStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source"
        );
        Map<String, List<Tuple4<Integer,Integer,Integer,Double>>> carOnePoint = new HashMap<>();
        Set<String> activeCarnumbers = new HashSet<>();

        // 处理数据
        kafkaStream
            .flatMap(new FlatMapFunction<String, Tuple4<String, String ,Long,List<Tuple4<Integer,Integer,Integer,Double>>>>() {
                  Map<String,List<Tuple4<Integer,Integer,Integer,Double>>> map = new LinkedHashMap<>();
                  Map<String,Long> mapJudge = new HashMap<>();
                  Map<String,String> mapTimeSeg = new HashMap<>();
                  Map<String,String> mapType = new HashMap<>();
                  Map<String,Long> mapstarttime = new HashMap<>();

                @Override
                public void flatMap(String jsonString, Collector<Tuple4<String, String ,Long,List<Tuple4<Integer,Integer,Integer,Double>>>> out) throws Exception {
                    try {
                  Map<String,Long> temp = new HashMap<>();
                        long startTime = 1737017338001L;
                        // 解析 JSON 数据
                        JSONObject jsonObject = new JSONObject(jsonString);
                        long timeobs = jsonObject.getLong("TIME");

                        // 提取车牌号列表
                        JSONArray tdataArray = jsonObject.getJSONArray("TDATA");
                            String type="";

                        for (int i = 0; i < tdataArray.length(); i++) {
                            JSONObject tdataObject = tdataArray.getJSONObject(i);
                            String carnumber=tdataObject.getString("Carnumber");

                             temp.put(carnumber,timeobs);
                             mapJudge.put(carnumber,timeobs);
                             if(map.get(carnumber)==null){//carnumber第一次出现
                                if (tdataObject.getInt("Boolean") == 0) {type="小车";} else {type="货车";}//type
                                 mapTimeSeg.put(carnumber,timeobs + "-"+carnumber);
                                 mapType.put(carnumber,type);
                                 mapstarttime.put(carnumber,timeobs);
                                List<Tuple4<Integer,Integer,Integer,Double>> list1 = new ArrayList<>();
                                list1.add(new Tuple4<>(tdataObject.getInt("Tpointno"),
                                        tdataObject.getInt("Wayno"),tdataObject.getInt("Direct"),
                                        tdataObject.getDouble("Speed")));
                                        map.put(carnumber,list1);
                             }else {
                                  List<Tuple4<Integer,Integer,Integer,Double>> list1FromMap = map.get(carnumber);
                                    list1FromMap.add(new Tuple4<>(tdataObject.getInt("Tpointno"),
                                        tdataObject.getInt("Wayno"),tdataObject.getInt("Direct"),
                                        tdataObject.getDouble("Speed")));
                             }
                             Map<String,Long> temp1 = mapJudge;
                             Set<String> tt = new HashSet<>(temp.keySet());
                             temp1.keySet().removeAll(tt);
                             if(!temp1.isEmpty()) {
                                 Set<String> keys = temp1.keySet();
                                  for (String key : keys) {//key是carnumber
                                        out.collect(new Tuple4<>(mapTimeSeg.get(key), mapType.get(key),timeobs,map.get(carnumber)));
                                  }
                                  mapJudge=temp;
                             }
                        }

                        // 将结果发送到下游
                    } catch (Exception e) {
                        // 输入不是有效 JSON 时忽略
                        System.err.println("解析 JSON 时出错: " + e.getMessage());
                    }
                }
            })
            .keyBy(tuple -> tuple.f0) // 按 timeSeg 分组
            .print();

        // 执行 Flink 作业
        env.execute("Flink Kafka JSON Processing");
    }

    public static long calRange(long timeobs, long timesplit, long starttime) {
        long index = (timeobs - starttime) / timesplit;
        return starttime + timesplit * index;
    }
       public static long getEight(long input) {
        String numStr = String.valueOf(input);
        return Long.parseLong(numStr.substring(numStr.length() - 8));
    }
      public static String getLastNString(String input,int num) {
        return input.substring(input.length() - num);
    }
}
