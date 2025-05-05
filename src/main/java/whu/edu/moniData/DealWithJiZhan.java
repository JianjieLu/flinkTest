package whu.edu.moniData;

import com.alibaba.fastjson2.JSON;
import javafx.util.Pair;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import whu.edu.ljj.flink.utils.JsonReader;
import whu.edu.ljj.flink.utils.LocationOP;
import whu.edu.ljj.flink.utils.myTools;
import whu.edu.ljj.flink.xiaohanying.Utils.*;
import whu.edu.moniData.Utils.TrafficEventUtils;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


public class DealWithJiZhan {
    private static long temp = 0;
    static List<Location> roadDataList;
    static {
        try {
             roadDataList = JsonReader.readJsonFile("ABCDK_locations.json");
          } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    //MergedPathData.sceneTest.1 "fiberDataTest1", "fiberDataTest2", "fiberDataTest3" 100.65.38.139:9092
    public static void main(String[] args) throws Exception {
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {
            env.setParallelism(3);
            String brokers = args[0];
            List<String> topics  = Arrays.asList(Arrays.copyOfRange(args, 1, args.length));;
            // 从Kafka读取数据
            DataStreamSource<String> kafkaStream = env.fromSource(buildSource(brokers, topics), WatermarkStrategy.noWatermarks(), "Kafka Source1");

//读取基站数据，返回StationStream
            DataStream<PathTData> StationStream=kafkaStream.flatMap((String jsonStr, Collector<PathTData> out)-> {
                try {
                    StationData data =null;
                    //这是基站数据
                    if(myTools.getNString(jsonStr,2,10).equals("frameNum")){
                        data = JSON.parseObject(jsonStr, StationData.class);
                        String gloTime=data.getGlobalTime();
                        temp = initCurrentTime(gloTime);
                        //问题：是不是这个globaltime,orgcode这么用吗？有问题
                        PathTData p = transStationToPathTDate(data, gloTime);
                        out.collect(p);
                    }
                } catch (Exception e) {
                    System.err.println("JSON解析失败: " + jsonStr);
                }
            }).returns(PathTData.class).keyBy(PathTData::getTime);

            SingleOutputStreamOperator<PathTData> endPathTDataStream = StationStream.flatMap(new FlatMapFunction<PathTData, PathTData>() {

                    @Override//5.56   33.76  86.64
                    public void flatMap(PathTData pathTData, Collector<PathTData> collector) throws Exception {
                        for(PathPoint p:pathTData.getPathList()){
                            p.setStakeId(LocationOP.UseLLGetSK(p.getLatitude(), p.getLongitude(), roadDataList).getKey().getLocation());

                        }

                    }//flatMap

                }
            );

            env.execute("Flink completion");
        }//flink env

    }//main

    private static PathTData transStationToPathTDate(StationData data, String gloTime) {
        List<PathPoint> plist=new ArrayList<>();
        for(StationTarget s: data.getTargetList()){
            //mileage\originalType\originalColor
            int mileage=s.getEnGap();
            double lon=s.getLon();
            double lat=s.getLat();

            PathPoint pp=new PathPoint(1,s.getId(),s.getLane(),mileage , s.getPicLicense()+s.getId(),s.getSpeed(), gloTime,s.getCarColor(),s.getCarType(),lon,lat,s.getAngle(),"skate",1,1);
            plist.add(pp);
        }
        PathTData p=new PathTData(data.getTargetList().size(), temp, gloTime, data.getOrgCode() , data.getOrgCode(),plist);
        return p;
    }


    private static KafkaSource<String> buildSource(String brokers, List<String> topics){
        String groupId = "flink_consumer_group";

        return KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(topics)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("message.max.bytes", "16777216")
                .setProperty("max.partition.fetch.bytes", "16777216")
                .build();
    }

    public static long initCurrentTime(String time){
        try {
            // 尝试按三位毫秒格式解析
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS");
            LocalDateTime localDateTime = LocalDateTime.parse(time, formatter);
            return localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        } catch (Exception e) {
            // 若三位毫秒格式解析失败，尝试按两位毫秒格式解析
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SS");
            LocalDateTime localDateTime = LocalDateTime.parse(time, formatter);
            return localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        }
    }


    public static void writeIntoKafka(SingleOutputStreamOperator<PathTData> endPathTDataStream){
        KafkaSink<String> dealStreamSink = KafkaSink.<String>builder()
                .setBootstrapServers("100.65.38.139:9092")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("completed.pathdata")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setProperty("max.request.size", "629145600") // 20MB
                .setProperty("message.max.bytes", "629145600") // Kafka Broker 的 message.max.bytes
                .build();

        DataStream<String> jsonStream = endPathTDataStream
                .map(JSON::toJSONString)
                .returns(String.class);

        jsonStream.sinkTo(dealStreamSink);
    }
}//public buquan class