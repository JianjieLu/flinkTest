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
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class CarTrajStorageJob {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String brokers = "100.65.38.40:9092";
        String groupId = "storage-group";
        String topic = "trajectoryoutput";

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setProperty("auto.offset.commit", "true")
                .setProperty("consumer.max.poll.interval.ms", String.valueOf(24 * 60 * 60 * 1000))
                .setProperty("session.timeout.ms", String.valueOf(24 * 60 * 60 * 1000))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> kafkaStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        kafkaStream
                .flatMap(new FlatMapFunction<String, Tuple5<String, Integer, Long, List<Tuple4<Double, Double, Integer, Double>>, Integer>>() {
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
                            System.err.println("解析轨迹JSON时出错: " + e.getMessage());
                        }
                    }
                })
                .addSink(new DynamicHBaseSink("ZCarTraj", "cf0"));

        env.execute("Trajectory Storage Job");
    }

    private static class DynamicHBaseSink extends RichSinkFunction<Tuple5<String, Integer, Long, List<Tuple4<Double, Double, Integer, Double>>, Integer>> {

        private final String baseTableName;
        private final String columnFamily;

        private transient Connection connection;
        private transient Table table;

        private transient String currentTableName;
        private transient LocalDateTime nextTableSwitchTime;
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
            conf.set("fs.defaultFS", "hdfs://100.65.38.139:9000");
            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            connection = ConnectionFactory.createConnection(conf);
            currentTableName = null;
            nextTableSwitchTime = null;
        }

        @Override
        public void invoke(Tuple5<String, Integer, Long, List<Tuple4<Double, Double, Integer, Double>>, Integer> value, SinkFunction.Context context) throws Exception {
            tableLock.lock();
            try {
                String rowKey = value.f0;
                long rowKeyTime = Long.parseLong(value.f0.split("-")[0]);

                // 切换表（如果必要）
                if (currentTableName == null || isTimeToSwitch(rowKeyTime)) {
                    switchTable(rowKeyTime);
                }

                // 插入数据
                Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("type"), Bytes.toBytes(value.f1.toString()));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("latest_time"), Bytes.toBytes(value.f2.toString()));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("trajectory"), Bytes.toBytes(value.f3.toString()));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("direction"), Bytes.toBytes(value.f4.toString()));
                table.put(put);
            } finally {
                tableLock.unlock();
            }
        }

        @Override
        public void close() throws Exception {
            if (table != null) {
                table.close();
            }
            if (connection != null) {
                connection.close();
            }
        }

        private boolean isTimeToSwitch(long rowKeyTime) {
            LocalDateTime rowKeyDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(rowKeyTime), ZoneId.systemDefault());
            return rowKeyDateTime.isAfter(nextTableSwitchTime);
        }

        private void switchTable(long rowKeyTime) throws IOException {
            tableLock.lock();
            try {
                LocalDateTime rowKeyDateTime = LocalDateTime.ofInstant(
                        Instant.ofEpochMilli(rowKeyTime), ZoneId.systemDefault()
                );

                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
                currentTableName = baseTableName + "_" + rowKeyDateTime.format(formatter);
                nextTableSwitchTime = rowKeyDateTime.toLocalDate().atStartOfDay().plusDays(1);

                createTableIfNotExists(currentTableName, columnFamily);
                if (table != null) table.close();
                table = connection.getTable(TableName.valueOf(currentTableName));

                System.out.printf("切换到新表: %s，下一次切换时间: %s%n",
                        currentTableName, nextTableSwitchTime.format(DateTimeFormatter.ISO_LOCAL_DATE));
            } finally {
                tableLock.unlock();
            }
        }

        public void createTableIfNotExists(String tableName, String columnFamily) throws IOException {
            tableLock.lock();
            try (Admin admin = connection.getAdmin()) {
                TableName hbaseTableName = TableName.valueOf(tableName);
                Object lock = tableCreationLocks.computeIfAbsent(tableName, k -> new Object());

                synchronized (lock) {
                    if (!admin.tableExists(hbaseTableName)) {
                        HTableDescriptor tableDescriptor = new HTableDescriptor(hbaseTableName);
                        tableDescriptor.addFamily(new HColumnDescriptor(columnFamily));
                        admin.createTable(tableDescriptor);
                    }
                }
            } finally {
                tableLock.unlock();
            }
        }
    }
}