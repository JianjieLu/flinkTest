package whu.edu.moniData;

import com.alibaba.fastjson2.JSON;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import lombok.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
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
import whu.edu.ljj.flink.xiaohanying.Utils.*;

import java.io.IOException;
import java.io.Serializable;
import java.text.DecimalFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DealWithJiZhanWithTimeUsed {
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

    public static void main(String[] args) throws Exception {
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {
            env.setParallelism(1);
            env.disableOperatorChaining();
            String brokers = args[0];
            List<String> topics  = Arrays.asList(Arrays.copyOfRange(args, 1, args.length));

            // 从Kafka读取数据
            DataStreamSource<String> kafkaStream = env.fromSource(
                    buildSource(brokers, topics),
                    WatermarkStrategy.noWatermarks(),
                    "Kafka Source1"
            );

            // 添加时间戳记录：记录数据进入系统的时间
            DataStream<Tuple2<String, Long>> timedStream = kafkaStream
                    .map(record -> {
                        long entryTime = System.currentTimeMillis();
                        return Tuple2.of(record, entryTime);
                    })
                    .returns(Types.TUPLE(Types.STRING, Types.LONG));

            // 处理基站数据
            DataStream<PathTData> StationStream = timedStream.flatMap(
                    (Tuple2<String, Long> tuple, Collector<PathTData> out) -> {
                        String jsonStr = tuple.f0;
                        long entryTime = tuple.f1;  // 获取入口时间戳
                        try {
                            StationData data = JSON.parseObject(jsonStr, StationData.class);
                            String gloTime = data.getGlobalTime();
                            temp = initCurrentTime(gloTime);
                            PathTData p = transStationToPathTDate(data, gloTime);

                            // 设置入口时间戳到PathTData对象
                            p.setEntryTime(entryTime);
                            out.collect(p);
                        } catch (Exception e) {
                            System.err.println("JSON解析失败: " + e.getMessage());
                        }
                    }
            ).returns(PathTData.class).keyBy(PathTData::getTime);

            // 处理流程
            SingleOutputStreamOperator<PathTData> endPathTDataStream = StationStream.flatMap(
                    new FlatMapFunction<PathTData, PathTData>() {
                        @Override
                        public void flatMap(PathTData pathTData, Collector<PathTData> collector) {
                            collector.collect(pathTData);
                        }
                    }
            );

            // 添加处理耗时计算
            SingleOutputStreamOperator<PathTData> processedStream = endPathTDataStream
                    .map(data -> {
                        long exitTime = System.currentTimeMillis();
                        long duration = exitTime - data.getEntryTime();

                        // 打印耗时统计信息
                        System.out.println("[耗时统计] 总处理时间: " + duration + "ms | " +
                                "数据时间: " + data.getTimeStamp() + " | " +
                                "入口时间: " + data.getEntryTime() + " | " +
                                "出口时间: " + exitTime);

                        return data;
                    });

            // 写入Kafka
            writeIntoKafka(processedStream);

            env.execute("Dealing with JiZhan Data(e1_data_XG01)");
        }
    }

    private static PathTData transStationToPathTDate(StationData data, String gloTime) throws IOException {
        List<PathPoint> plist = new ArrayList<>();
        for (StationTarget s : data.getTargetList()) {
            double lon = s.getLon();
            double lat = s.getLat();
            String stake = LocationOP.UseLLGetSK(lat, lon, roadDataList).getKey().getLocation();
            PathPoint pp = new PathPoint();
            pp.setId(s.getId());
            pp.setMileage(stakeToMileage(stake));
            pp.setLaneNo(s.getLane());

            if (s.getPicLicense() != null) pp.setPlateNo(s.getPicLicense());
            pp.setSpeed(s.getSpeed());
            pp.setTimeStamp(gloTime);
            pp.setPlateColor(s.getCarColor());
            pp.setOriginalType(s.getCarType());
            pp.setLongitude(lon);
            pp.setLatitude(lat);
            pp.setCarAngle(Double.parseDouble(df.format(s.getAngle())));
            pp.setStakeId("K1122+200-" + stake);
            pp.setWeight(0);
            plist.add(pp);
        }
        PathTData p = new PathTData();
        p.setPathList(plist);
        p.setPathNum(plist.size());
        p.setTime(temp);
        p.setTimeStamp(gloTime);
        return p;
    }

    private static KafkaSource<String> buildSource(String brokers, List<String> topics) {
        String groupId = "flink_consumer_group";

        return KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(topics)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("message.max.bytes", "16777216")
                .setProperty("max.partition.fetch.bytes", "16777216")
                .setProperty("consumer.max.poll.interval.ms", String.valueOf(24 * 60 * 60 * 1000))
                .setProperty("session.timeout.ms", String.valueOf(24 * 60 * 60 * 1000))
                .build();
    }

    public static long initCurrentTime(String time) {
        try {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS");
            LocalDateTime localDateTime = LocalDateTime.parse(time, formatter);
            return localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        } catch (Exception e) {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SS");
            LocalDateTime localDateTime = LocalDateTime.parse(time, formatter);
            return localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        }
    }

    public static void writeIntoKafka(SingleOutputStreamOperator<PathTData> endPathTDataStream) {
        KafkaSink<String> secondarySink = KafkaSink.<String>builder()
                .setBootstrapServers("10.48.53.82:9092")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("MergedRampPathData")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setProperty("max.request.size", "629145600")
                .setProperty("delivery.timeout.ms", String.valueOf(24 * 60 * 60 * 1000))
                .build();

        DataStream<String> jsonStream = endPathTDataStream
                .map(JSON::toJSONString)
                .returns(String.class);

        jsonStream.sinkTo(secondarySink);
    }

    private static double stakeToMileage(String input) {
        String[] parts = input.split("\\+");
        if (parts.length != 2) {
            throw new IllegalArgumentException("输入格式无效，应包含一个加号分隔符");
        }

        String frontPart = parts[0];
        String rearPart = parts[1];

        List<String> numbers = new ArrayList<>();
        Matcher matcher = Pattern.compile("\\d+").matcher(frontPart);
        while (matcher.find()) {
            numbers.add(matcher.group());
        }
        if (numbers.isEmpty()) {
            throw new IllegalArgumentException("前段中未找到数字");
        }
        int prefix = Integer.parseInt(numbers.get(numbers.size() - 1));

        float suffix = Float.parseFloat(rearPart);

        return (int) (prefix * 1000 + suffix);
    }
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    @Setter
    @JsonPropertyOrder({
            "timeStamp",
            "pathNum",
            "pathList",
            "time",
            "waySectionId",
            "waySectionName"
    })
    public static class PathTData implements Serializable {
        private Integer pathNum;
        private long time;
        private String timeStamp;
        private String waySectionId;
        private String waySectionName;
        private List<PathPoint> pathList;
        long EntryTime;
    }
}