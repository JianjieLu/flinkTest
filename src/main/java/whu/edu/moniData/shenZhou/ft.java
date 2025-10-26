package whu.edu.moniData.shenZhou;


import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import whu.edu.ljj.flink.xiaohanying.Utils.PathPoint;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import com.alibaba.fastjson2.JSONArray;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import whu.edu.ljj.flink.xiaohanying.Utils;
//flink run -c whu.edu.moniData.ingest.holyAnalysisJob.redisAndHbase.hourlyJobWithZaDaily /home/ljj/totalInfo/flinkTest-1.0-SNAPSHOT.jar  100.65.38.40:9092 e1_data_XG01


public class ft {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // ==================== 主路数据处理 ====================
        // Kafka配置 - 主路数据
        String brokers = "10.48.53.82:9092";
        String groupId = "hourly-traffic-group";
        List<String> mainRoadTopics = Arrays.asList("jtkj.jga.path.1");

        // 创建Kafka源 - 主路数据
        KafkaSource<String> mainRoadKafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(mainRoadTopics)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 主路数据流
        DataStream<String> mainRoadSourceStream = env.fromSource(
                mainRoadKafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Main Road Kafka Source"
        );

        SingleOutputStreamOperator<PathPoint> mainRoadPathPointStream = mainRoadSourceStream
                .flatMap(new FlatMapFunction<String, PathPoint>() {
                    @Override
                    public void flatMap(String value, Collector<PathPoint> out) {
                        try {
                            JSONObject json = JSON.parseObject(value);
                            String timestamp = json.getString("timeStamp");
                            JSONArray pathList = json.getJSONArray("pathList");
                            for (int i = 0; i < pathList.size(); i++) {
                                PathPoint point = pathList.getObject(i, PathPoint.class);
                                point.setTimeStamp(timestamp);
                                out.collect(point);
                            }
                            System.out.println(pathList);
                        } catch (Exception e) {
                            System.err.println("Error parsing JSON: " + e.getMessage());
                        }
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<PathPoint>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((event, recordTimestamp) ->
                                        Utils.convertToTimestampMillis(event.getTimeStamp()))
                )
                .name("MainRoadPathPointStream");
        env.execute("123");
        }

}
