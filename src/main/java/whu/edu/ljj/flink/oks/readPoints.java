package whu.edu.ljj.flink.oks;
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
import org.apache.flink.util.Collector;
import static whu.edu.ljj.flink.xiaohanying.Utils.*;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static whu.edu.ljj.flink.xiaohanying.Utils.*;

public class readPoints {
    private static final int WINDOW_SIZE = 10;//用来预测的窗口大小
    private static final Map<Long, VehicleData> vehicleMap = new ConcurrentHashMap<>();
    static int timeout=300;
    private static final int TIMEOUT_MS = 2000; // 超时时间
    private static int ssn=0;
    public static void main(String[] args) throws Exception {
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {

            // 配置Kafka连接信息
            String brokers = "100.65.38.40:9092";
            String topic = "MergedPathData";
            String groupId = "flink_consumer_group";

            // 创建Kafka数据源
            KafkaSource<String> source = KafkaSource.<String>builder()
                    .setBootstrapServers(brokers)
                    .setTopics(topic)
                    .setGroupId(groupId)
                    .setStartingOffsets(OffsetsInitializer.latest())
                    .setValueOnlyDeserializer(new SimpleStringSchema())
                    .build();

            // 从Kafka读取数据
            DataStreamSource<String> kafkaStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
            DataStream<PathTData> parsedStream = kafkaStream
                    .flatMap((String jsonStr, Collector<PathTData> out) -> {
                        try {
                            System.out.println(jsonStr);
//                            PathTData data = JSON.parseObject(jsonStr, PathTData.class);
//                            out.collect(data);
                        } catch (Exception e) {
                            System.err.println("JSON解析失败: " + jsonStr);
                        }
                    }).returns(PathTData.class);
            parsedStream.flatMap(new FlatMapFunction<PathTData, Object>() {
                @Override
                public void flatMap(PathTData mergeData, Collector<Object> collector) throws Exception {
                    myTools.printOneMergeData(mergeData);
//                    System.out.println(mergeData.getTime());
//                    System.out.println(mergeData.getTimeStamp());
                }
            });
            // 打印读取的数据
//            kafkaStream.print();
//            kafkaStream.map()
            // 执行任务
            env.execute("Flink Read Kafka");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}