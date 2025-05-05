package whu.edu.ljj.flink.merge;

import javafx.util.Pair;

import static whu.edu.ljj.flink.merge.tools.JsonConverter.*;
import static whu.edu.ljj.flink.xiaohanying.Utils.*;

import org.apache.commons.collections.ListUtils;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import whu.edu.ljj.flink.utils.JsonReader;
import whu.edu.ljj.flink.utils.myTools;
import com.alibaba.fastjson2.JSON;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import whu.edu.ljj.flink.utils.LocationOP;
import org.apache.flink.util.Collector;

import java.io.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
        import java.util.concurrent.ConcurrentHashMap;

public class Transfer {
    private static final int WINDOW_SIZE = 20;//用来预测的窗口大小
    private static final Map<Long, PathPointData> PointMap = new ConcurrentHashMap<>();
    private static final Map<Long, PathPointData> JizhanPointMap = new ConcurrentHashMap<>();//雷视数据获取到的匝道上的所有车
    static boolean firstEnter=true;
    static Map<Long, Pair<Boolean,Integer>> nowMap= new ConcurrentHashMap<>();
    //    carid  是否在路上  数据缺失了几次
    static Map<Pair<Long,String>,String> zaMap= new ConcurrentHashMap<>();
    //       carid  carNumber  匝道编号
    private static final long mainRoadMinMillage=0;//主路上的最小里程
    private static final long mainRoadMaxMillage=1111111111;//主路上的最大里程
    static List<Location> roadKDataList;
    static List<Location> roadAKDataList;
    static List<Location> roadBKDataList;
    static List<Location> roadCKDataList;
    static List<Location> roadDKDataList;
    private static String pathTimeStamp="";
    private static float predictedSpeed=0;//预测速度
    private static double distanceDiff=0;
    private static long pathTime=0;
    private static int newscount=0;
    private static boolean iii=true;
    private static PathTData patda;

    static {
        try {
            roadKDataList  = JsonReader.readJsonFile("D:\\learn\\codes\\a_idea_codes\\flinkTest\\src\\main\\java\\whu\\edu\\ljj\\flink\\data\\zadaoGeojson\\K_locations.json");
            roadAKDataList = JsonReader.readJsonFile("D:\\learn\\codes\\a_idea_codes\\flinkTest\\src\\main\\java\\whu\\edu\\ljj\\flink\\data\\zadaoGeojson\\AK_locations.json");
            roadBKDataList = JsonReader.readJsonFile("D:\\learn\\codes\\a_idea_codes\\flinkTest\\src\\main\\java\\whu\\edu\\ljj\\flink\\data\\zadaoGeojson\\BK_locations.json");
            roadCKDataList = JsonReader.readJsonFile("D:\\learn\\codes\\a_idea_codes\\flinkTest\\src\\main\\java\\whu\\edu\\ljj\\flink\\data\\zadaoGeojson\\CK_locations.json");
            roadDKDataList = JsonReader.readJsonFile("D:\\learn\\codes\\a_idea_codes\\flinkTest\\src\\main\\java\\whu\\edu\\ljj\\flink\\data\\zadaoGeojson\\DK_locations.json");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    public static void main(String[] args) throws Exception {
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {
            env.setParallelism(3);
            // 配置Kafka连接信息
            String brokers = "100.65.38.40:9092";
            String groupId = "flink_consumer_group";
            List<String> topics = Arrays.asList("MergedPathData");
//            List<String> topics = Collections.singletonList("news-topic");
//            List<String> topics = Collections.singletonList("MergedPathData.sceneTest.1");
            // 创建Kafka数据源
            KafkaSource<String> source = KafkaSource.<String>builder()
                    .setBootstrapServers(brokers)
                    .setTopics(topics)
                    .setGroupId(groupId)
                    .setStartingOffsets(OffsetsInitializer.latest())
                    .setValueOnlyDeserializer(new SimpleStringSchema())
                    .build();

            // 从Kafka读取数据
            DataStreamSource<String> kafkaStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source1");
            DataStream<PathTData> parsedStream = kafkaStream
            .flatMap((String jsonStr, Collector<PathTData> out) -> {
                try {
                    PathTData data =null;

                    //验证，如果json的前几位是timestamp，则认为是mergedata
                    if(myTools.getNString(jsonStr,2,11).equals("timeStamp")) {

                        try (BufferedWriter writer1 = new BufferedWriter(new FileWriter("D:\\learn\\codes\\a_idea_codes\\flinkTest\\src\\main\\java\\whu\\edu\\ljj\\flink\\merge\\data\\data01\\04031558.txt",true))) {
                            writer1.write(jsonStr);
                            writer1.write(System.lineSeparator());
                        }


                    }

                    out.collect(data);
//                            }
                } catch (Exception e) {
                    System.err.println("JSON解析失败: " + jsonStr);
                }
            }).returns(PathTData.class).keyBy(PathTData::getTime);

//            // 执行任务
            env.execute("Flink Read Kafka");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
