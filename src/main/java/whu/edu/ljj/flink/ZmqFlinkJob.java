package whu.edu.ljj.flink;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import static whu.edu.ljj.flink.xiaohanying.Utils.*;

public class ZmqFlinkJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String zmqAddress = "tcp://100.65.62.82:8030"; // ZMQ服务地址

//        env.addSource(new ZmqSource(zmqAddress))
//                .map(message -> "Received: " + message)
//                .print();

        DataStream<TrajeData> trajeStream = env.addSource(new ZmqSource("tcp://100.65.62.82:8030"))
                .map(new MapFunction<JSONObject, TrajeData>() {
                    @Override
                    public TrajeData map(JSONObject value) throws Exception {
                        String trajeJSON = value.toString();
                        if(!trajeJSON.equals("{}"))
                            return JSON.parseObject(trajeJSON, TrajeData.class);
                        else {
                            TrajeData noData = new TrajeData();
                            noData.setSN(-1);
                            return noData;
                        }
                    }
                });

        env.execute("Flink ZMQ Consumer");
    }
}