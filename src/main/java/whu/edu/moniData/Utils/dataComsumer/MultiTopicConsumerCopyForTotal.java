package whu.edu.moniData.Utils.dataComsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONObject;

import java.time.Duration;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Properties;

public class MultiTopicConsumerCopyForTotal {
    // 定义时间格式解析器（线程安全）
    private static final DateTimeFormatter formatter =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS");
    public static void main(String[] args) {
        // 1. 创建消费者配置
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.48.53.82:9092");
//        props.put("bootstrap.servers", "100.65.38.40:9092");
//        props.put("bootstrap.servers", "100.65.38.139:9092");
//        props.put("bootstrap.servers", "192.168.0.5:9092");
        props.put("group.id", "my-consuming-group1");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("auto.offset.reset", "latest");
        // 2. 创建消费者实例
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            // 3. 订阅多个Topic
//            consumer.subscribe(Arrays.asList("trajectoryoutput"));
//            consumer.subscribe(Arrays.asList("trajectoryoutput"));
//            consumer.subscribe(Arrays.asList("specialTrafficInfo"));
//            consumer.subscribe(Arrays.asList("e1_data_XG01"));
//            consumer.subscribe(Arrays.asList("smartBS_xg"));
//            consumer.subscribe(Arrays.asList("wd.platform.en.ex.vehicles"));
//            consumer.subscribe(Arrays.asList("vehicle.trajectories"));
//            consumer.subscribe(Arrays.asList(".five.min.trajectories"));
//            consumer.subscribe(Arrays.asList("UDPDecoder"));
//            consumer.subscribe(Arrays.asList("MergedRampPathData"));
            consumer.subscribe(Arrays.asList("jtkj.jga.path"));

            consumer.subscribe(Arrays.asList("traffic_events"));
//            consumer.subscribe(Arrays.asList("low_speed_events"));
//            consumer.subscribe(Arrays.asList("jtkj.jga.path"));

//            consumer.subscribe(Arrays.asList("fiberData1","fiberData2","fiberData3","fiberData4","fiberData5","fiberData6","fiberData7","fiberData8","fiberData9","fiberData10","fiberData11"));
//            consumer.subscribe(Arrays.asList("fiberData1"));
//            consumer.subscribe(Arrays.asList());
//            consumer.subscribe(Arrays.asList("zaOutPut"));
//            consumer.subscribe(Arrays.asList("QueueLengthOutput"));
//            consumer.subscribe(Arrays.asList("trajectoryoutput"));
//            consumer.subscribe(Arrays.asList("traffic_metrics_minutely"));

//            consumer.subscribe(Arrays.asList("smartBS_xg"));
//            consumer.subscribe(Arrays.asList("bs3"));
//            consumer.subscribe(Arrays.asList("bs16"));
//            consumer.subscribe(Arrays.asList("MergedPathData.sceneTest.2"));
//            consumer.subscribe(Collections.singletonList("completed.pathdata"));
            // 4. 持续轮询消息
            while (true) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(100));
                // 5. 处理每条消息
                for (ConsumerRecord<String, String> record : records) {
                    JSONObject jsonObj = new JSONObject(record.value());
//                    int deid = jsonObj.getJSONArray("trajectory").length();
//                    String timestampStr = jsonObj.getString("timeStamp");
//                    if(deid==3){
                        // 输出消息信息
//                    if(deid<10)
                        System.out.printf("\n收到消息: \n"
                                        + "Topic: %s\n"
                                        + "Partition: %d\n"
                                        + "Offset: %d\n"
                                        + "Key: %s\n"
                                        + "数据: %s\n",
                                record.topic(),
                                record.partition(),
                                record.offset(),
                                record.key(),
                                record.value()
                        );
//                    }


                }
            }
        }
    }
}



