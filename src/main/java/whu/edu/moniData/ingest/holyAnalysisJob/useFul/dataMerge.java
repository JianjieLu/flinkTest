package whu.edu.moniData.ingest.holyAnalysisJob.useFul;
import static whu.edu.moniData.shenZhou.Utils.*;

import com.alibaba.fastjson2.JSON;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

import java.time.Duration;
import java.util.*;

public class dataMerge {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<String> topics = Arrays.asList(
                "fiberData1",
                "fiberData2",
                "fiberData3",
                "fiberData4",
                "fiberData5",
                "fiberData6",
                "fiberData7",
                "fiberData8",
                "fiberData9",
                "fiberData10",
                "fiberData11"
        );

        KafkaSource<String> fiberDataTestSource = KafkaSource.<String>builder()
                .setTopics(topics.get(0))
                .setBootstrapServers("10.48.53.82:9092")
                .setGroupId("fiberDataTest-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setProperty("auto.offset.commit", "true")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<PathTData> unionStream = env.fromSource(fiberDataTestSource,
                        WatermarkStrategy.noWatermarks(),
                        "fiberDataTest Source " + 1)
                .setParallelism(1)
                .map(trajejson -> {
                    PathTData pathData = JSON.parseObject(trajejson, PathTData.class);
                    pathData.setTime(convertToTimestampMillis(pathData.getTimeStamp()));
                    pathData.setSegId(1);
                    return pathData;
                }).setParallelism(1);
//        unionStream.map(value -> {
//                System.out.println("unionStream里的数据为："+value);
//                return value;
//            });

        // 按照topics顺序创建 Kafka 数据源
        for(int i = 1; i < topics.size(); i++) {
            KafkaSource<String> source = KafkaSource.<String>builder()
                    .setTopics(topics.get(i))
                    .setBootstrapServers("10.48.53.82:9092")
                    .setGroupId("fiberDataTest-group")
                    .setStartingOffsets(OffsetsInitializer.latest())
                    .setProperty("auto.offset.commit", "true")
                    .setValueOnlyDeserializer(new SimpleStringSchema())
                    .build();
            // 使用局部final变量，以保证lambda表达式可以正常使用
            final int segmentId = i + 1;
            DataStream<PathTData> stream = env.fromSource(source,
                            WatermarkStrategy.noWatermarks(),
                            "fiberDataTest Source " + segmentId)
                    .setParallelism(1)
                    .map(trajejson -> {
                        PathTData pathData = JSON.parseObject(trajejson, PathTData.class);
                        pathData.setTime(convertToTimestampMillis(pathData.getTimeStamp()));
                        if (segmentId == 5)
                            pathData.setSegId(segmentId * 5);
                        else if(segmentId == 7)
                            pathData.setSegId(segmentId * 7 - 2);
                        else if(segmentId == 8)
                            pathData.setSegId(segmentId * 3);
                        else
                            pathData.setSegId(segmentId);
                        return pathData;
                    }).setParallelism(1);

            // 合并11段路段
            SingleOutputStreamOperator<PathTData> mergedPathTDataStream = stream
                    .assignTimestampsAndWatermarks(WatermarkStrategy.<PathTData>forBoundedOutOfOrderness(Duration.ofMillis(300))
                            .withTimestampAssigner(new SerializableTimestampAssigner<PathTData>() {
                                                       @Override
                                                       public long extractTimestamp(PathTData pathData, long recordTimestamp) {
                                                           return pathData.getTime();
                                                       }
                                                   }
                            ).withIdleness(Duration.ofSeconds(10)))
                    .keyBy(PathTData::getSegId).windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.milliseconds(200))) // 200ms滚动窗口
                    .aggregate(new AggregateFunction<PathTData, PathTData, PathTData>() {

                        @Override
                        public PathTData createAccumulator() {
                            List<PathPoint> ppointList = new ArrayList<>();
                            // 返回结果中不能有SegId，这里设置为null
                            return new PathTData(0, 0L, "", null, ppointList);
                        }

                        @Override
                        public PathTData add(PathTData value, PathTData accumulator) {
                            if (accumulator.getTime() == 0L)
                                accumulator.setTime(value.getTime());
                            if (Objects.equals(accumulator.getTimeStamp(), ""))
                                accumulator.setTimeStamp(value.getTimeStamp());
                            accumulator.getPathList().addAll(value.getPathList());
                            accumulator.setPathNum(accumulator.getPathNum() + value.getPathNum());
                            return accumulator;
                        }

                        @Override
                        public PathTData getResult(PathTData accumulator) {
                            return accumulator;
                        }

                        @Override
                        public PathTData merge(PathTData a, PathTData b) {
                            System.out.println("出现错误：聚合窗口中莫名的合并函数merge调用");
                            return null;
                        }
                    })
                    .setParallelism(1);
        }
    }
}
