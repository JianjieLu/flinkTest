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
import java.text.DecimalFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class DealWithJiZhan {
    private static long temp = 0;
    static List<Location> roadDataList;
    public static DecimalFormat df = new DecimalFormat("0.00");
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
            env.setParallelism(1);
            env.disableOperatorChaining();
            String brokers = args[0];
            List<String> topics  = Arrays.asList(Arrays.copyOfRange(args, 1, args.length));;
            // 从Kafka读取数据
            DataStreamSource<String> kafkaStream = env.fromSource(buildSource(brokers, topics), WatermarkStrategy.noWatermarks(), "Kafka Source1");

//读取基站数据，返回StationStream
            DataStream<PathTData> StationStream=kafkaStream.flatMap((String jsonStr, Collector<PathTData> out)-> {
                try {
                    StationData data =null;
                    //这是基站数据

                        data = JSON.parseObject(jsonStr, StationData.class);
                        String gloTime=data.getGlobalTime();
                        temp = initCurrentTime(gloTime);
                        //问题：是不是这个globaltime,orgcode这么用吗？有问题
                        PathTData p = transStationToPathTDate(data, gloTime);
                        out.collect(p);
                } catch (Exception e) {
                    System.err.println("JSON解析失败: " +"====");
                }
            }).returns(PathTData.class).keyBy(PathTData::getTime);

            SingleOutputStreamOperator<PathTData> endPathTDataStream = StationStream.flatMap(new FlatMapFunction<PathTData, PathTData>() {
                 @Override//5.56   33.76  86.64
                 public void flatMap(PathTData pathTData, Collector<PathTData> collector) throws Exception {
                     collector.collect(pathTData);
                 }//flatMap
             }
            );
            writeIntoKafka(endPathTDataStream);
            env.execute("Dealing with JiZhan Data(e1_data_XG01)");
        }//flink env

    }//main

    private static PathTData transStationToPathTDate(StationData data, String gloTime) throws IOException {
        List<PathPoint> plist=new ArrayList<>();
        for(StationTarget s: data.getTargetList()){
            //mileage\originalType\originalColor
            double lon=s.getLon();
            double lat=s.getLat();
            String stake=LocationOP.UseLLGetSK(lat, lon, roadDataList).getKey().getLocation();
            PathPoint pp=new PathPoint();
            pp.setId(s.getId());
            pp.setMileage(stakeToMileage(stake));
            pp.setLaneNo(s.getLane());

            if(s.getPicLicense()!=null)pp.setPlateNo(s.getPicLicense());
            pp.setSpeed(s.getSpeed());
            pp.setTimeStamp(gloTime);
            pp.setPlateColor(s.getCarColor());
            pp.setVehicleType(s.getCarType());
            pp.setLongitude(lon);
            pp.setLatitude(lat);
            pp.setCarAngle(Double.parseDouble(df.format(s.getAngle())));
            pp.setStakeId("K1122+200-"+stake);
            pp.setWeight(0);
            plist.add(pp);

        }
        PathTData p=new PathTData();
        p.setPathList(plist);
        p.setPathNum(plist.size());
        p.setTime(temp);
        p.setTimeStamp(gloTime);

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
                .setProperty("consumer.max.poll.interval.ms",String.valueOf( 24*60*60*1000))
                .setProperty("session.timeout.ms",String.valueOf(24*60*60*1000))
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
                .setBootstrapServers("100.65.38.40:9092")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("MergedPathData")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setProperty("max.request.size", "629145600") // 20MB
                .setProperty("message.max.bytes", "629145600") // Kafka Broker 的 message.max.bytes
                .setProperty("delivery.timeout.ms",String.valueOf(24*60*60*1000))
                .build();
// 创建第二个KafkaSink（新集群）
        KafkaSink<String> secondarySink = KafkaSink.<String>builder()
                .setBootstrapServers("10.48.53.82:9092") // 新增集群
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("MergedPathData")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setProperty("max.request.size", "629145600")
                .setProperty("delivery.timeout.ms", String.valueOf(24 * 60 * 60 * 1000))
                .build();
        DataStream<String> jsonStream = endPathTDataStream
                .map(JSON::toJSONString)
                .returns(String.class);

        jsonStream.sinkTo(dealStreamSink);
        jsonStream.sinkTo(secondarySink);
    }
    private static int stakeToMileage(String input) {
        String[] parts = input.split("\\+");
        if (parts.length != 2) {
            throw new IllegalArgumentException("输入格式无效，应包含一个加号分隔符");
        }

        String frontPart = parts[0];
        String rearPart = parts[1];

        // 提取前段中的最后一个数字
        List<String> numbers = new ArrayList<>();
        Matcher matcher = Pattern.compile("\\d+").matcher(frontPart);
        while (matcher.find()) {
            numbers.add(matcher.group());
        }
        if (numbers.isEmpty()) {
            throw new IllegalArgumentException("前段中未找到数字");
        }
        int prefix = Integer.parseInt(numbers.get(numbers.size() - 1));

        // 处理后段数字
        float suffix = Float.parseFloat(rearPart);

        return (int) (prefix * 1000 + suffix);
    }
}//public buquan class