package whu.edu.moniData;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONObject;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Properties;

public class MultiTopicConsumerCopy {
    // 定义时间格式解析器（线程安全）
    private static final DateTimeFormatter formatter =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS");

    // 存储上一条消息的时间戳（毫秒）
    private static long lastTimestampMillis = 0;

    public static void main(String[] args) {
        // 1. 创建消费者配置
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.48.53.82:9092");
        props.put("group.id", "my-consuming-group");
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
//            consumer.subscribe(Arrays.asList("UDPDecoder"));
//            consumer.subscribe(Arrays.asList("MergedPathData"));
            consumer.subscribe(Arrays.asList("fiberData1","fiberData2","fiberData3","fiberData4","fiberData5","fiberData6","fiberData7","fiberData8","fiberData9","fiberData10","fiberData11"));
//            consumer.subscribe(Arrays.asList("fiberData1"));
//            consumer.subscribe(Arrays.asList("trajectoryoutput"));
//            consumer.subscribe(Arrays.asList("completed.pathdata"));
            consumer.subscribe(Arrays.asList("MergedPathData.sceneTest.2"));
            // 4. 持续轮询消息
            while (true) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(100));

                // 5. 处理每条消息
                for (ConsumerRecord<String, String> record : records) {
                    // 解析JSON
                    JSONObject jsonObj = new JSONObject(record.value());

                    // 获取车辆数和时间戳
                    int vehicleCount = jsonObj.getJSONArray("pathList").length();

                    String timestampStr = jsonObj.getString("timeStamp");

                    // 解析时间字符串为LocalDateTime
                    LocalDateTime currentTime = LocalDateTime.parse(timestampStr, formatter);
                    long currentTimestampMillis = currentTime.atZone(java.time.ZoneId.systemDefault())
                            .toInstant()
                            .toEpochMilli();

                    // 计算时间差（第一条消息差值为0）
                    long timeDiff = 0;
                    if (lastTimestampMillis != 0) {
                        timeDiff = currentTimestampMillis - lastTimestampMillis;
                    }
                    lastTimestampMillis = currentTimestampMillis;

                    // 输出消息信息
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

                    // 输出车辆数和时间差
                    System.out.println("车辆数量: " + vehicleCount);
                    System.out.println("与上条消息时间差: " + timeDiff + " 毫秒");
                }
            }
        }
    }
}