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
    public static void main(String[] args) throws Exception {
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {
            env.setParallelism(3);
            // 配置Kafka连接信息
//            String brokers = "100.65.38.40:9092";
            String brokers = "10.48.53.82:9092";
            String groupId = "flink_consumer_group";
            List<String> topics = Arrays.asList("MergedRampPathData");
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
                            System.out.println(jsonStr);
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
