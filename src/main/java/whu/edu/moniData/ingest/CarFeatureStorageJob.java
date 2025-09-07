package whu.edu.moniData.ingest;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.Arrays;
import java.util.List;

public class CarFeatureStorageJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // 设置为1确保状态一致性

        // Kafka配置
        String brokers = "10.48.53.82:9092";
        String groupId = "feature-group";
        List<String> topics = Arrays.asList(
                "fiberData1", "fiberData2", "fiberData3",
                "fiberData4", "fiberData5", "fiberData6",
                "fiberData7", "fiberData8", "fiberData9",
                "fiberData10", "fiberData11");

        // 创建Kafka源
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(topics.get(0))
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> unionStream = env.fromSource(
                kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source 1");

        // 添加其他主题
        for (int i = 1; i < topics.size(); i++) {
            KafkaSource<String> source = KafkaSource.<String>builder()
                    .setBootstrapServers(brokers)
                    .setTopics(topics.get(i))
                    .setGroupId(groupId)
                    .setStartingOffsets(OffsetsInitializer.latest())
                    .setValueOnlyDeserializer(new SimpleStringSchema())
                    .build();

            DataStream<String> stream = env.fromSource(
                    source, WatermarkStrategy.noWatermarks(), "Kafka Source " + (i + 1));
            unionStream = unionStream.union(stream);
        }

        // 1. 首先提取特征（无状态操作）
        DataStream<Tuple5<String, String, Integer, Integer, Double>> featureStream =
                unionStream.flatMap(new FeatureExtractor());

        // 2. 按键分区（使用rowKey作为键）
        KeyedStream<Tuple5<String, String, Integer, Integer, Double>, String> keyedStream =
                featureStream.keyBy(value -> value.f0);

        // 3. 使用状态去重
        DataStream<Tuple5<String, String, Integer, Integer, Double>> deduplicatedStream =
                keyedStream.flatMap(new Deduplicator());

        // 4. 写入HBase
        deduplicatedStream.addSink(new CarFeatureHBaseSink());

        env.execute("Car Feature Storage Job");
    }

    /**
     * 特征提取器 - 无状态操作
     */
    private static class FeatureExtractor implements FlatMapFunction<String, Tuple5<String, String, Integer, Integer, Double>> {
        @Override
        public void flatMap(String jsonString, Collector<Tuple5<String, String, Integer, Integer, Double>> out) {
            try {
                JSONObject jsonObject = new JSONObject(jsonString);
                JSONArray pathList = jsonObject.getJSONArray("pathList");

                for (int i = 0; i < pathList.length(); i++) {
                    JSONObject vehicle = pathList.getJSONObject(i);

                    // 生成rowkey: 车牌号 + ID
                    String plateNo = vehicle.getString("plateNo");
                    long id = vehicle.getLong("id");
                    String rowKey = plateNo + id;

                    // 提取特征
                    String specialFlag = getSpecialFlagSafely(vehicle);
                    int vehicleColor = getVehicleColorSafely(vehicle);
                    int vehicleType = vehicle.getInt("vehicleType");
                    double vehicleWeight = getVehicleWeightSafely(vehicle);

                    // 发送到下游
                    out.collect(new Tuple5<>(rowKey, specialFlag, vehicleColor, vehicleType, vehicleWeight));
                }
            } catch (JSONException e) {
                System.err.println("JSON解析错误: " + e.getMessage());
            } catch (Exception e) {
                System.err.println("数据处理错误: " + e.getMessage());
            }
        }

        private int getVehicleColorSafely(JSONObject vehicle) {
            try {
                return vehicle.getInt("vehicleColor");
            } catch (JSONException e) {
                return 0; // 默认值
            }
        }

        private double getVehicleWeightSafely(JSONObject vehicle) {
            try {
                return vehicle.getDouble("vehicleWeight");
            } catch (JSONException e) {
                return 0.0; // 默认值
            }
        }

        private String getSpecialFlagSafely(JSONObject vehicle) {
            try {
                return vehicle.getString("specialFlag");
            } catch (JSONException e) {
                return "0"; // 默认值
            }
        }
    }

    /**
     * 去重器 - 使用键控状态实现TTL
     */
    private static class Deduplicator extends RichFlatMapFunction<Tuple5<String, String, Integer, Integer, Double>,
            Tuple5<String, String, Integer, Integer, Double>> {

        private ValueState<Boolean> processedState;

        @Override
        public void open(Configuration parameters) {
            // 配置状态TTL (1天)
            StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.days(1))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .cleanupInRocksdbCompactFilter(1000) // 在RocksDB压缩时清理
                    .build();

            // 创建状态描述符
            ValueStateDescriptor<Boolean> descriptor =
                    new ValueStateDescriptor<>("processedState", Boolean.class);
            descriptor.enableTimeToLive(ttlConfig);

            // 初始化状态
            processedState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void flatMap(Tuple5<String, String, Integer, Integer, Double> feature,
                            Collector<Tuple5<String, String, Integer, Integer, Double>> out) throws Exception {

            // 检查是否已处理过该车辆
            if (processedState.value() == null) {
                // 发送到下游
                out.collect(feature);

                // 标记为已处理（自动设置1天TTL）
                processedState.update(true);
            }
        }
    }

    /**
     * HBase Sink - 将车辆特征写入Car_Feature表
     */
    private static class CarFeatureHBaseSink extends RichSinkFunction<Tuple5<String, String, Integer, Integer, Double>> {

        private Connection connection;
        private Table table;
        private final String tableName = "Car_Feature";
        private final String columnFamily = "cf1";

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            super.open(parameters);

            // HBase配置
            org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142,10.48.53.80");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            conf.set("zookeeper.session.timeout", "120000");

            connection = ConnectionFactory.createConnection(conf);

            // 确保表存在
            try (Admin admin = connection.getAdmin()) {
                TableName hbaseTableName = TableName.valueOf(tableName);
                if (!admin.tableExists(hbaseTableName)) {
                    // 创建表描述符
                    HTableDescriptor tableDescriptor = new HTableDescriptor(hbaseTableName);
                    // 添加列族
                    HColumnDescriptor columnDescriptor = new HColumnDescriptor(columnFamily);
                    tableDescriptor.addFamily(columnDescriptor);

                    // 创建表
                    admin.createTable(tableDescriptor);
                    System.out.println("表创建成功: " + tableName);
                }
                table = connection.getTable(hbaseTableName);
            }
        }

        @Override
        public void invoke(Tuple5<String, String, Integer, Integer, Double> feature, Context context) throws Exception {
            // rowKey = 车牌号 + ID
            String rowKey = feature.f0;
            Put put = new Put(Bytes.toBytes(rowKey));

            // 添加特征数据
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("specialFlag"), Bytes.toBytes(feature.f1));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("vehicleColor"), Bytes.toBytes(feature.f2));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("vehicleType"), Bytes.toBytes(feature.f3));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("vehicleWeight"), Bytes.toBytes(feature.f4));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("event_list"), Bytes.toBytes("[]")); // 空事件列表

            // 写入HBase
            table.put(put);
            System.out.println("写入特征数据: " + rowKey);
        }

        @Override
        public void close() throws Exception {
            if (table != null) table.close();
            if (connection != null) connection.close();
            super.close();
        }
    }
}