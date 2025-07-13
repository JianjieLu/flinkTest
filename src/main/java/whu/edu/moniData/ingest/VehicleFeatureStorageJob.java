package whu.edu.moniData.ingest;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.json.JSONArray;
import org.json.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;

public class VehicleFeatureStorageJob {

    // HBase配置
    private static final String HBASE_ZOOKEEPER_QUORUM = "100.65.38.40,100.65.38.41,100.65.38.42";
    private static final String HBASE_TABLE_NAME = "vehicle_features";
    private static final byte[] CF = Bytes.toBytes("cf");
    private static final byte[] PLATE_COLOR_COL = Bytes.toBytes("plateColor");
    private static final byte[] VEHICLE_TYPE_COL = Bytes.toBytes("vehicleType");
    private static final byte[] LAST_SEEN_COL = Bytes.toBytes("lastSeen");
    private static final byte[] SPEED_COL = Bytes.toBytes("speed");

    private static Connection hbaseConnection = null;

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 初始化HBase连接
        initHBaseConnection();

        // 添加关闭钩子释放资源
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                if (hbaseConnection != null) {
                    hbaseConnection.close();
                }
            } catch (IOException e) {
                System.err.println("Error closing HBase connection: " + e.getMessage());
            }
        }));

        // Kafka配置
        String brokers = "100.65.38.40:9092";
        String groupId = "vehicle-feature-group";
        List<String> topics = Arrays.asList("fiberData1", "fiberData2", "fiberData3",
                "fiberData4", "fiberData5", "fiberData6", "fiberData7",
                "fiberData8", "fiberData9", "fiberData10", "fiberData11");

        // 创建Kafka源
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(topics)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> sourceStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka Vehicle Feature Source"
        );

        // 处理数据流并存储到HBase
        sourceStream.process(new ProcessFunction<String, Void>() {
            @Override
            public void processElement(String jsonString, Context ctx, Collector<Void> out) throws Exception {
                try {
                    JSONObject jsonObject = new JSONObject(jsonString);
                    JSONArray pathList = jsonObject.getJSONArray("pathList");
                    String timestamp = jsonObject.getString("timeStamp");

                    for (int i = 0; i < pathList.length(); i++) {
                        JSONObject vehicleData = pathList.getJSONObject(i);

                        // 提取车辆特征
                        String plateNo = vehicleData.getString("plateNo");
                        int plateColor = vehicleData.getInt("plateColor");
                        int vehicleType = vehicleData.getInt("vehicleType");
                        double speed = vehicleData.getDouble("speed");

                        // 转换时间戳格式
                        LocalDateTime localDateTime = parseTimestamp(timestamp);
                        String formattedTimestamp = localDateTime.format(
                                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                        );

                        // 存储到HBase
                        storeVehicleFeature(plateNo, plateColor, vehicleType, speed, formattedTimestamp);
                    }
                } catch (Exception e) {
                    System.err.println("Error processing vehicle data: " + e.getMessage());
                }
            }
        });

        env.execute("Vehicle Feature Storage Job");
    }

    // 初始化HBase连接
    private static synchronized void initHBaseConnection() throws IOException {
        if (hbaseConnection == null || hbaseConnection.isClosed()) {
            Configuration config = HBaseConfiguration.create();
            config.set("hbase.zookeeper.quorum", HBASE_ZOOKEEPER_QUORUM);
            config.set("hbase.zookeeper.property.clientPort", "2181");
            hbaseConnection = ConnectionFactory.createConnection(config);
        }
    }

    // 解析时间戳
    private static LocalDateTime parseTimestamp(String timestamp) {
        try {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS");
            return LocalDateTime.parse(timestamp, formatter);
        } catch (Exception e1) {
            try {
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SS");
                return LocalDateTime.parse(timestamp, formatter);
            } catch (Exception e2) {
                return LocalDateTime.now(ZoneId.systemDefault());
            }
        }
    }

    // 存储车辆特征到HBase
    private static void storeVehicleFeature(String plateNo, int plateColor, int vehicleType,
                                            double speed, String timestamp) {
        try {
            Table table = hbaseConnection.getTable(TableName.valueOf(HBASE_TABLE_NAME));

            // 创建Put对象，使用车牌号作为RowKey
            Put put = new Put(Bytes.toBytes(plateNo));

            // 添加列数据
            put.addColumn(CF, PLATE_COLOR_COL, Bytes.toBytes(plateColor));
            put.addColumn(CF, VEHICLE_TYPE_COL, Bytes.toBytes(vehicleType));
            put.addColumn(CF, SPEED_COL, Bytes.toBytes(speed));
            put.addColumn(CF, LAST_SEEN_COL, Bytes.toBytes(timestamp));

            // 写入HBase
            table.put(put);
            table.close();

            System.out.println("Stored vehicle feature: " + plateNo);
        } catch (IOException e) {
            System.err.println("HBase write error for vehicle " + plateNo + ": " + e.getMessage());
        }
    }
}