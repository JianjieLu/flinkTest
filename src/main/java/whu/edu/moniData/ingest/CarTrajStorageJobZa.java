package whu.edu.moniData.ingest;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class CarTrajStorageJobZa {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 增加消费者健康检查参数
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "100.65.38.40:9092");
        kafkaProps.setProperty("group.id", "storage-group");
        kafkaProps.setProperty("max.poll.interval.ms", "300000");    // 5分钟
        kafkaProps.setProperty("session.timeout.ms", "10000");       // 10秒
        kafkaProps.setProperty("heartbeat.interval.ms", "3000");     // 3秒
        kafkaProps.setProperty("auto.offset.reset", "latest");
        kafkaProps.setProperty("allow.auto.create.topics", "true");  // 允许自动创建topic

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setProperties(kafkaProps)
                .setTopics("zaOutPut")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.latest())
                .build();

        DataStream<String> kafkaStream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source"
        ).setParallelism(2);  // 设置并行度

        kafkaStream.flatMap(new JSONParser())
                .addSink(new DynamicHBaseSink("ZZaCarTraj", "cf0"))
                .setParallelism(3);  // 增加Sink并行度

        env.execute("Za Trajectory Storage Job");
    }

    /**
     * 提取JSON解析逻辑到独立类，便于错误处理
     */
    private static class JSONParser implements FlatMapFunction<String, Tuple5<String, Integer, Long, List<Tuple4<Double, Double, Integer, Double>>, Integer>> {
        @Override
        public void flatMap(String jsonString, Collector<Tuple5<String, Integer, Long, List<Tuple4<Double, Double, Integer, Double>>, Integer>> out) {
            try {
                JSONObject jsonObject = new JSONObject(jsonString);
                String timeSeg = jsonObject.getString("timeSeg");
                int type = jsonObject.getInt("type");
                long latestTime = jsonObject.getLong("latestTime");
                JSONArray trajectoryArray = jsonObject.getJSONArray("trajectory");
                int dir = trajectoryArray.getJSONObject(0).getInt("direction");

                List<Tuple4<Double, Double, Integer, Double>> trajectory = new ArrayList<>();
                for (int i = 0; i < trajectoryArray.length(); i++) {
                    JSONObject point = trajectoryArray.getJSONObject(i);
                    trajectory.add(new Tuple4<>(
                            point.getDouble("longitude"),
                            point.getDouble("latitude"),
                            point.getInt("laneNo"),
                            point.getDouble("speed")
                    ));
                }
                out.collect(new Tuple5<>(timeSeg, type, latestTime, trajectory, dir));
            } catch (JSONException e) {
                System.err.println("ERROR: JSON解析失败 - " + e.getMessage());
                System.err.println("原始数据: " + jsonString);
            }
        }
    }

    private static class DynamicHBaseSink extends RichSinkFunction<Tuple5<String, Integer, Long, List<Tuple4<Double, Double, Integer, Double>>, Integer>> {
        private final String baseTableName;
        private final String columnFamily;
        private transient Connection connection;
        private transient Table currentTable;
        private transient String currentTableName;
        private final ReentrantLock tableLock = new ReentrantLock();
        private static final ConcurrentHashMap<String, Object> tableCreationLocks = new ConcurrentHashMap<>();

        public DynamicHBaseSink(String baseTableName, String columnFamily) {
            this.baseTableName = baseTableName;
            this.columnFamily = columnFamily;
        }

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            super.open(parameters);
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142,10.48.53.80");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            conf.set("zookeeper.session.timeout", "120000");
            conf.set("hbase.rpc.timeout", "300000");
            connection = ConnectionFactory.createConnection(conf);
        }

        @Override
        public void invoke(Tuple5<String, Integer, Long, List<Tuple4<Double, Double, Integer, Double>>, Integer> value, Context context) throws Exception {
            String rowKey = value.f0;
            long rowKeyTime;
            try {
                rowKeyTime = Long.parseLong(value.f0.split("-")[0]);
            } catch (NumberFormatException e) {
                System.err.println("ERROR: 无效的rowKey格式: " + value.f0);
                return;
            }

            switchTableIfNeeded(rowKeyTime);

            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("type"), Bytes.toBytes(value.f1.toString()));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("latest_time"), Bytes.toBytes(value.f2.toString()));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("trajectory"), Bytes.toBytes(value.f3.toString()));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("direction"), Bytes.toBytes(value.f4.toString()));

            try {
                currentTable.put(put);
            } catch (IOException e) {
                System.err.println("ERROR: HBase写入失败 - " + e.getMessage());
                resetConnection();
            }
        }

        private void switchTableIfNeeded(long rowKeyTime) throws IOException {
            LocalDateTime rowKeyDateTime = LocalDateTime.ofInstant(
                    Instant.ofEpochMilli(rowKeyTime), ZoneId.systemDefault()
            );
            String newTableName = baseTableName + "_" + rowKeyDateTime.format(DateTimeFormatter.BASIC_ISO_DATE);

            if (currentTable == null || !newTableName.equals(currentTableName)) {
                tableLock.lock();
                try {
                    if (currentTable == null || !newTableName.equals(currentTableName)) {
                        createTableIfNotExists(newTableName);
                        if (currentTable != null) currentTable.close();
                        currentTable = connection.getTable(TableName.valueOf(newTableName));
                        currentTableName = newTableName;
                        System.out.println("切换HBase表: " + currentTableName);
                    }
                } finally {
                    tableLock.unlock();
                }
            }
        }

        // 使用旧版API创建表
        private void createTableIfNotExists(String tableName) throws IOException {
            Object lock = tableCreationLocks.computeIfAbsent(tableName, k -> new Object());
            synchronized (lock) {
                try (Admin admin = connection.getAdmin()) {
                    TableName tn = TableName.valueOf(tableName);
                    if (!admin.tableExists(tn)) {
                        // 旧版API实现
                        HTableDescriptor tableDescriptor = new HTableDescriptor(tn);
                        HColumnDescriptor cfDesc = new HColumnDescriptor(columnFamily);
                        tableDescriptor.addFamily(cfDesc);
                        admin.createTable(tableDescriptor);
                        System.out.println("创建HBase表: " + tableName);
                    }
                } catch (IOException e) {
                    System.err.println("ERROR: 创建HBase表失败 - " + e.getMessage());
                    throw e;
                }
            }
        }

        private void resetConnection() {
            try {
                if (connection != null) connection.close();
                Configuration conf = HBaseConfiguration.create();
                conf.set("hbase.zookeeper.quorum", "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142,10.48.53.80");
                connection = ConnectionFactory.createConnection(conf);
                if (currentTableName != null) {
                    currentTable = connection.getTable(TableName.valueOf(currentTableName));
                }
            } catch (IOException ex) {
                System.err.println("ERROR: 重建HBase连接失败 - " + ex.getMessage());
            }
        }

        @Override
        public void close() throws Exception {
            try {
                if (currentTable != null) currentTable.close();
            } finally {
                if (connection != null) connection.close();
            }
        }

    }
}