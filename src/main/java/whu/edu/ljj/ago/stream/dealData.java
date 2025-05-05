package whu.edu.ljj.ago.stream;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class dealData {
    public static void putOne(String tableName,String rowkey, String colFamily, String qualifier, String value) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.0.5");  // Zookeeper 地址
        conf.set("hbase.zookeeper.property.clientPort", "2181");  // Zookeeper 端口
        // 获取连接
        try (Connection connection = ConnectionFactory.createConnection(conf);
             Table table = connection.getTable(TableName.valueOf(tableName))) {
            Put put = new Put(Bytes.toBytes(rowkey));
            put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(qualifier), Bytes.toBytes(value));
            table.put(put);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
        public static void main(String[] args) throws Exception {
        String topic = args[0];//topic
        String timeSplit = args[1];//路段车辆表时间间隔
        // 设置 Flink 流执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 配置 KafkaSource
        String bootstrapServers = "192.168.0.5:9092"; // Kafka集群地址
        String groupId = "flink-group"; // 消费者组ID

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest()) // 从最早的偏移量开始
                .setValueOnlyDeserializer(new SimpleStringSchema()) // 反序列化消息为String
                .build();

        // 从 KafkaSource 创建数据流
        DataStreamSource<String> kafkaStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source"
        );
        long startTime = (long) (17370221.339*10000);

        // 处理数据
          kafkaStream.map(jsonString -> {
        int id = 1;
        try {
            // 解析 JSON 数据
            JSONObject jsonObject = new JSONObject(jsonString);
            // 提取 TimeObs (TIME 字段)
            long timeObs = jsonObject.getLong("TIME");
            // 提取 segID (SN 字段)
            int segID = jsonObject.getInt("SN");
            // 提取 listCarId (TDATA 中的 Carnumber 列表)
            JSONArray tdataArray = jsonObject.getJSONArray("TDATA");
            List<String> listCarId = new ArrayList<>();
            for (int i = 0; i < tdataArray.length(); i++) {
                JSONObject tdataObject = tdataArray.getJSONObject(i);
                listCarId.add(tdataObject.getString("Carnumber"));
            }
            // 提取 count (COUNT 字段)
            int count = jsonObject.getInt("COUNT");
            // 打印 "路段时间表"
            System.out.println("===============路段时间表===============");
            System.out.println("TimeObs: " + timeObs);
            System.out.println("segID: " + segID);
            System.out.println("listCarId: " + listCarId);
            System.out.println("count: " + count);
            // 打印 "车辆轨迹表"
            System.out.println("===============车辆轨迹表===============");
            for (int i = 0; i < tdataArray.length(); i++) {
                System.out.println("-----车辆轨迹表：" + id + "-----");
                JSONObject tdataObject = tdataArray.getJSONObject(i);
                System.out.println("carID: " + id++);
                System.out.println("carNumber: " + tdataObject.getString("Carnumber"));
                // 根据 Boolean 判断车辆类型
                if (tdataObject.getInt("Boolean") == 0) {
                    System.out.println("车辆类型： 小车");
                } else {
                    System.out.println("车辆类型： 货车");
                }
                // 打印轨迹信息
                System.out.println("观测时间： " + timeObs);
                System.out.println("轨迹点： <Tpointno, Wayno, Direct, Speed> = " +
                        tdataObject.getInt("Tpointno") + ", " +
                        tdataObject.getInt("Wayno") + ", " +
                        tdataObject.getInt("Direct") + ", " +
                        tdataObject.getDouble("Speed"));
            }
             System.out.println("===============end===============\n");
        } catch (org.json.JSONException e) {
            // 输入不是 JSON
            System.out.println("输入不为 JSON");
        }
        return null;
    });
        // 执行 Flink 作业
        env.execute("Flink Kafka JSON Processing");

        }
}
