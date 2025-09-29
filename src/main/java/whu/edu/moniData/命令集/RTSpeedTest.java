package whu.edu.moniData.命令集;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.*;

import whu.edu.moniData.Utils.TrafficEventUtils.*;
import static whu.edu.moniData.shenZhou.Utils.*;

/**
 * RTTrafficSituationV6
 * 6.16
 *  1. 接入新模拟数据
 *  2. 过滤车型
 * 9.10
 *  1. 加入应急车道 - laneNo9（应急车道的车辆行为特征？应急车道非紧急情况绝不占用）
 *  2. 开启checkPoints
 * 9.20
 *  设置接入测试道路
 */
public class RTSpeedTest {
    private static final Logger LOG = LoggerFactory.getLogger(RTSpeedTest.class);

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://namenode:9000/flink/checkpoints/rtSeg");
        env.getConfig().setAutoWatermarkInterval(100); // 每 100ms 生成一次水位线
        // 测试一下
        env.setParallelism(4);

        // 配置 KafkaSource
        String brokers = "10.48.53.82:9092";
        String groupId = "flink-group-RTExactlyOnceWithCP"; // 消费者组ID

        // 主题列表
        List<String> topics = Arrays.asList("jtkj.jga.path");

        // 初始化第一个 KafkaSource
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(topics)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setProperty("auto.offset.commit", "true")
                .setProperty("consumer.max.poll.interval.ms", String.valueOf(24*60*60*1000)) // 1 天
                .setProperty("session.timeout.ms", String.valueOf(24*60*60*1000)) // 1 天T
                .setProperty("heartbeat.interval.ms", "30000") // 30 秒
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 创建第一个数据流
        DataStream<String> unionStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Sources RTTraffic");

        unionStream.map(v->{
            System.out.println(JSON.parseObject(v).getString("timeStamp"));
            return null;
        });

        env.execute("Real-Time traffic on HDFS - RTExactlyOnceWithCP");
    }

    public static double keep2Digits(double number) {
        return Math.round(number * 100.0) / 100.0;
    }
}
