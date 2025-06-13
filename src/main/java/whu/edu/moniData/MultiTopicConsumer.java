package whu.edu.moniData;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class MultiTopicConsumer {
    public static void main(String[] args) {
        // 1. 创建消费者配置
        Properties props = new Properties();
        props.put("bootstrap.servers", "100.65.38.40:9092");  // 修正参数名
        props.put("group.id", "my-consuming-group");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("auto.offset.reset", "latest");
        // 2. 创建消费者实例
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {

            // 3. 订阅多个Topic
            consumer.subscribe(Arrays.asList("e1_data_XG01"));
//            consumer.subscribe(Arrays.asList("smartBS_xg"));
//            consumer.subscribe(Arrays.asList("UDPDecoder"));
//            consumer.subscribe(Arrays.asList("MergedPathData"));
//            consumer.subscribe(Arrays.asList("fiberData1"));
//            consumer.subscribe(Arrays.asList("completed.pathdata"));
//            consumer.subscribe(Arrays.asList("MergedPathData.sceneTest.2"));

            // 4. 持续轮询消息
            while (true) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(100));  // 使用新版API

                // 5. 处理每条消息
                for (ConsumerRecord<String, String> record : records) {




                    System.out.printf("\n收到消息: \n"
                                    + "Topic: %s\n"
                                    + "Partition: %d\n"
                                    + "Offset: %d\n"
                                    + "Key: %s\n"
                                    + "Value: %s\n",
                            record.topic(),
                            record.partition(),
                            record.offset(),
                            record.key(),
                            record.value());
                }
            }
        }
    }
}


//收到消息:
//Topic: e1_data_XG01
//Partition: 0
//Offset: 27238806
//Key: e1_data:2025.06.11-15.00.47.918
//Value: {"orgCode":"XG01","frameNum":312496,"targetNum":5,"targetList":
//        [{"id":304939,"carType":10,"carColor":0,"lane":6,"station":8,"lon":114.0367285,"lat":30.9192423,"speed":0.0,"angle":91.2,"picLicense":"鄂K18715","firstReceiveTime":"2025-06-11 14:59:12.047","enGap":0,"licenseColor":1,"speedAvg":0.0,"passTime":95,"disBefore":-1,"inOrOut":2},
//        {"id":304941,"carType":10,"carColor":0,"lane":19,"station":10,"lon":114.0412795,"lat":30.9190653,"speed":44.0,"angle":89.9,"picLicense":"鄂A8119K","firstReceiveTime":"2025-06-11 14:59:44.678","enGap":0,"licenseColor":0,"speedAvg":0.0,"passTime":63,"disBefore":190,"inOrOut":1}
//        ,{"id":304942,"carType":1,"carColor":0,"lane":19,"station":11,"lon":114.0431675,"lat":30.9184982,"speed":51.0,"angle":136.7,"picLicense":"鄂LC2138","firstReceiveTime":"2025-06-11 15:00:04.768","enGap":0,"licenseColor":0,"speedAvg":0.0,"passTime":43,"disBefore":-1,"inOrOut":1},
//        {"id":304943,"carType":10,"carColor":0,"lane":19,"station":7,"lon":114.0392006,"lat":30.9190624,"speed":25.0,"angle":82.9,"picLicense":"豫QF6580","firstReceiveTime":"2025-06-11 15:00:05.916","enGap":0,"licenseColor":1,"speedAvg":0.0,"passTime":42,"disBefore":198,"inOrOut":1},
//        {"id":304945,"carType":1,"carColor":0,"lane":4,"station":10,"lon":114.0409528,"lat":30.9191927,"speed":56.0,"angle":270.9,"picLicense":"鄂EH97M8","firstReceiveTime":"2025-06-11 15:00:20.141","enGap":0,"licenseColor":0,"speedAvg":0.0,"passTime":27,"disBefore":-1,"inOrOut":2}],
//        "globalTime":"2025-06-11 15:00:47:896"}
//
//收到消息:
//Topic: e1_data_XG01
//Partition: 0
//Offset: 27238807
//Key: e1_data:2025.06.11-15.00.48.027
//Value: {"orgCode":"XG01","frameNum":312497,"targetNum":5,"targetList":[
//        {"id":304939,"carType":10,"carColor":0,"lane":6,"station":8,"lon":114.0367283,"lat":30.9192423,"speed":4.0,"angle":91.3,"picLicense":"鄂K18715","firstReceiveTime":"2025-06-11 14:59:12.047","enGap":0,"licenseColor":1,"speedAvg":0.0,"passTime":95,"disBefore":-1,"inOrOut":2},
//        {"id":304941,"carType":10,"carColor":0,"lane":19,"station":10,"lon":114.0412926,"lat":30.9190653,"speed":44.0,"angle":89.9,"picLicense":"鄂A8119K","firstReceiveTime":"2025-06-11 14:59:44.678","enGap":0,"licenseColor":0,"speedAvg":0.0,"passTime":63,"disBefore":191,"inOrOut":1},
//        {"id":304942,"carType":1,"carColor":0,"lane":19,"station":11,"lon":114.0431783,"lat":30.9184882,"speed":51.0,"angle":136.7,"picLicense":"鄂LC2138","firstReceiveTime":"2025-06-11 15:00:04.768","enGap":0,"licenseColor":0,"speedAvg":0.0,"passTime":43,"disBefore":-1,"inOrOut":1},
//        {"id":304943,"carType":10,"carColor":0,"lane":19,"station":7,"lon":114.0392064,"lat":30.9190621,"speed":28.0,"angle":93.2,"picLicense":"豫QF6580","firstReceiveTime":"2025-06-11 15:00:05.916","enGap":0,"licenseColor":1,"speedAvg":0.0,"passTime":42,"disBefore":199,"inOrOut":1}
//        ,{"id":304945,"carType":1,"carColor":0,"lane":4,"station":10,"lon":114.0409345,"lat":30.9191933,"speed":56.0,"angle":270.9,"picLicense":"鄂EH97M8","firstReceiveTime":"2025-06-11 15:00:20.141","enGap":0,"licenseColor":0,"speedAvg":0.0,"passTime":27,"disBefore":-1,"inOrOut":2}],
//        "globalTime":"2025-06-11 15:00:48:007"}


//收到消息:
//Topic: smartBS_xg
//Partition: 0
//Offset: 585503
//Key: null
//Value: {"sequence":0,"mainCmd":225,"subCmd":2,"status":0,"msgLength":186,"deviceId":11,"reserved1":0,"frameNum":0,"timestampMicrosec":1749622895618,"longitude":30.9184681,"latitude":30.9184681,"angle":47.0,"participantCount":1,"reserved2":42679,
//        "participants":[{"id":62623,"type":6,"confidence":0.0,"color":0,"source":2,"signBit":3,"cameraId":0,"longitude":114.0434029,"latitude":30.9182715,"altitude":0.0,"speed":15.24,"heading":1.39,"length":5.29,"width":2.1599998,"height":2.3899999,"X":33.41,"Y":3.84,"Z":-4.2,"trackCount":0}],"frontCameraTs":0,"bodyCameraTs":0,"rearCameraTs":0,"reserved3":0,"checksum":244}
//
//收到消息:
//Topic: smartBS_xg
//Partition: 0
//Offset: 585504
//Key: null
//Value: {"sequence":0,"mainCmd":225,"subCmd":2,"status":0,"msgLength":186,"deviceId":7,"reserved1":0,"frameNum":0,"timestampMicrosec":1749622895805,"longitude":30.9190943,"latitude":30.9190943,"angle":0.0,"participantCount":1,"reserved2":0,
//        "participants":[{"id":1457,"type":0,"confidence":0.0,"color":0,"source":2,"signBit":0,"cameraId":0,"longitude":114.0386621,"latitude":30.9190608,"altitude":0.0,"speed":19.0,"heading":0.89,"length":4.31,"width":1.79,"height":1.5999999,"X":8.099999,"Y":3.58,"Z":-4.88,"trackCount":0}],"frontCameraTs":0,"bodyCameraTs":0,"rearCameraTs":0,"reserved3":0,"checksum":40}
//
//收到消息:
//Topic: smartBS_xg
//Partition: 0
//Offset: 585505
//Key: null
//Value: {"sequence":0,"mainCmd":225,"subCmd":2,"status":0,"msgLength":186,"deviceId":10,"reserved1":0,"frameNum":0,"timestampMicrosec":1749622926022,"longitude":30.9190196,"latitude":30.9190196,"angle":0.0,"participantCount":1,"reserved2":0
//        ,"participants":[{"id":4397,"type":0,"confidence":0.0,"color":0,"source":2,"signBit":3,"cameraId":0,"longitude":114.042007,"latitude":30.9190663,"altitude":0.0,"speed":13.58,"heading":0.96,"length":4.69,"width":1.88,"height":1.8499999,"X":56.07,"Y":2.74,"Z":-3.8999999,"trackCount":0}],"frontCameraTs":0,"bodyCameraTs":0,"rearCameraTs":0,"reserved3":0,"checksum":42}