package whu.edu.moniData.UDPRece;

import com.alibaba.fastjson2.JSON;
import lombok.*;
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
import whu.edu.ljj.flink.xiaohanying.Utils;

import java.io.IOException;
import java.text.DecimalFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static whu.edu.ljj.flink.xiaohanying.Utils.convertToTimestampString;

public class dealWithOriginalBaseStation {
    private static long temp = 0;
    static List<Utils.Location> roadDataList;
    public static DecimalFormat df = new DecimalFormat("0.00");
    static {
        try {
            roadDataList = JsonReader.readJsonFile("ABCDK_locations.json");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    public static void main(String[] args) throws Exception {
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {
            env.setParallelism(1);
            env.disableOperatorChaining();
            String brokers = args[0];
            List<String> topics  = Arrays.asList(Arrays.copyOfRange(args, 1, args.length));;
            // 从Kafka读取数据
            DataStreamSource<String> kafkaStream = env.fromSource(buildSource(brokers, topics), WatermarkStrategy.noWatermarks(), "Kafka Source1");

//读取基站数据，返回StationStream
            DataStream<Utils.PathTData> StationStream=kafkaStream.flatMap((String jsonStr, Collector<Utils.PathTData> out)-> {
                try {
                    UDPData data =null;
                    //这是基站数据

                    data = JSON.parseObject(jsonStr, UDPData.class);
                    temp =data.getTimestampMicrosec();
                    String gtime=convertToTimestampString(temp);
                    //问题：是不是这个globaltime,orgcode这么用吗？有问题
                    Utils.PathTData p = transStationToPathTDate(data, gtime);
                    out.collect(p);
                } catch (Exception e) {
                    System.err.println("JSON解析失败: " +"====");
                }
            }).returns(Utils.PathTData.class).keyBy(Utils.PathTData::getTime);

            SingleOutputStreamOperator<Utils.PathTData> endPathTDataStream = StationStream.flatMap(new FlatMapFunction<Utils.PathTData, Utils.PathTData>() {
                   @Override//5.56   33.76  86.64
                   public void flatMap(Utils.PathTData pathTData, Collector<Utils.PathTData> collector) throws Exception {
                       collector.collect(pathTData);
                   }//flatMap
               }
            );
            writeIntoKafka(endPathTDataStream);
            env.execute("original bs to :  UDPDecoder)");
        }//flink env

    }//main

    private static Utils.PathTData transStationToPathTDate(UDPData data, String gloTime) throws IOException {
        List<Utils.PathPoint> plist=new ArrayList<>();
        for(Participant s: data.getParticipants()){
            //mileage\originalType\originalColor
            double lon=s.getLongitude();
            double lat=s.getLatitude();
            String stake= LocationOP.UseLLGetSK(lat, lon, roadDataList).getKey().getLocation();
            Utils.PathPoint pp=new Utils.PathPoint();
            pp.setId(s.getId());
            pp.setMileage(stakeToMileage(stake));


            if(s.getPlateNo()!=null)pp.setPlateNo(s.getPlateNo());
            pp.setSpeed(s.getSpeed());
            pp.setTimeStamp(gloTime);
            pp.setPlateColor(s.getColor());
            pp.setVehicleType(s.getType());
            pp.setLongitude(lon);
            pp.setLatitude(lat);
            pp.setCarAngle(Double.parseDouble(df.format(s.getHeading())));
            pp.setStakeId(stake);
            plist.add(pp);

        }
        Utils.PathTData p=new Utils.PathTData();
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


    public static void writeIntoKafka(SingleOutputStreamOperator<Utils.PathTData> endPathTDataStream){
        KafkaSink<String> dealStreamSink = KafkaSink.<String>builder()
                .setBootstrapServers("100.65.38.40:9092")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("UDPDecoder")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setProperty("max.request.size", "629145600") // 20MB
                .setProperty("message.max.bytes", "629145600") // Kafka Broker 的 message.max.bytes
                .setProperty("delivery.timeout.ms",String.valueOf(24*60*60*1000))
                .build();

        DataStream<String> jsonStream = endPathTDataStream
                .map(JSON::toJSONString)
                .returns(String.class);

        jsonStream.sinkTo(dealStreamSink);
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

    // 文档要求的UDPData格式
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    @Setter
    static class UDPData {
        long sequence;
        int mainCmd;
        int subCmd;
        int status;
        int msgLength;
        int deviceId;
        int reserved1;
        long frameNum;
        long timestampMicrosec;
        double longitude;
        double latitude;
        float angle;
        int participantCount;
        int reserved2;
        List<Participant> participants = new ArrayList<>();
        long frontCameraTs;
        long bodyCameraTs;
        long rearCameraTs;
        int reserved3;
        int checksum;
    }

    // 文档要求的Participant格式
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    @Setter
    static class Participant {
        long id;
        int type;
        float confidence;
        int color;
        int source;
        int signBit;
        int cameraId;
        double longitude;
        double latitude;
        float altitude;
        float speed;
        float heading;
        float length;
        float width;
        float height;
        float X;
        float Y;
        float Z;
        int trackCount;

        String plateNo="";
    }

}

