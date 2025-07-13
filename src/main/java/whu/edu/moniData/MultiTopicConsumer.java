package whu.edu.moniData;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONArray;
import org.json.JSONObject;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class MultiTopicConsumer {
    public static void main(String[] args) {
        // 1. 创建消费者配置
        Properties props = new Properties();
//        props.put("bootstrap.servers", "100.65.38.40:9092");
        props.put("bootstrap.servers", "10.48.53.82:9092");
        props.put("group.id", "vehicle-monitoring-group");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("auto.offset.reset", "latest");

        // 2. 创建消费者实例
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            // 3. 订阅多个Topic
//            consumer.subscribe(Arrays.asList(
//                    "fiberData1", "fiberData2", "fiberData3",
//                    "fiberData4", "fiberData5", "fiberData6",
//                    "fiberData7", "fiberData8", "fiberData9",
//                    "fiberData10", "fiberData11"
//            ));
//            consumer.subscribe(Arrays.asList("specialTrafficInfo"));
            consumer.subscribe(Arrays.asList("MergedPathData"));

            System.out.println("开始监控车辆信息（车牌号 + 里程）...");
            System.out.println("格式: [主题] 时间戳 -> 车辆1, 车辆2, ...");
            System.out.println("========================================");

            // 4. 持续轮询消息
            while (true) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(100));

                // 5. 处理每条消息
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        // 解析JSON数据
                        JSONObject jsonObj = new JSONObject(record.value());
                        // 提取消息时间戳
                        String timestamp = jsonObj.getString("timeStamp");
                        // 获取车辆列表
                        JSONArray pathList = jsonObj.getJSONArray("pathList");
                        // 准备车辆信息字符串
                        StringBuilder vehicleInfo = new StringBuilder();

                        // 处理每辆车的信息
//                        for (int i = 0; i < pathList.length(); i++) {
//                            JSONObject vehicle = pathList.getJSONObject(i);
//
//                            // 提取车牌号和里程
//                            String plateNo = vehicle.getString("plateNo");
//                            int mileage = vehicle.getInt("mileage");
//
//                            // 添加到车辆信息字符串
//                            if (i > 0) vehicleInfo.append(", ");
//                            vehicleInfo.append(plateNo)
//                                    .append(" (K")
//                                    .append(mileage / 1000)
//                                    .append("+")
//                                    .append(String.format("%03d", mileage % 1000))
//                                    .append(")");
//                        }

                        // 单行输出车辆信息
                        System.out.printf("[%s] %s -> %s   %d %n",
                                record.topic(), timestamp, vehicleInfo.toString(),pathList.length());

                    } catch (Exception e) {
                        System.err.println("处理消息出错: " + e.getMessage());
                    }
                }
            }
        }
    }
}