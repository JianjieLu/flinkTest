package whu.edu.moniData.ingest.holyAnalysisJob;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
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

public class CombinedCarTrajStorageJob_NoFeature {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4); // 设置合理的并行度

        // ================== Kafka 配置 ==================
        String primaryBrokers = "10.48.53.82:9092";
        String secondaryBrokers = "10.48.53.82:9092";
        String groupId = "combined-storage-group";

        // ================== 主数据源 (trajectoryoutput) ==================
        KafkaSource<String> primarySource = KafkaSource.<String>builder()
                .setBootstrapServers(primaryBrokers)
                .setTopics("trajectoryoutput")
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setProperty("auto.offset.commit", "true")
                .setProperty("consumer.max.poll.interval.ms", String.valueOf(24 * 60 * 60 * 1000))
                .setProperty("session.timeout.ms", String.valueOf(24 * 60 * 60 * 1000))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> primaryStream = env.fromSource(
                primarySource, WatermarkStrategy.noWatermarks(), "Primary Kafka Source");

        // ================== 辅助数据源 (zaOutPut) ==================
        Properties secondaryProps = new Properties();
        secondaryProps.setProperty("bootstrap.servers", secondaryBrokers);
        secondaryProps.setProperty("group.id", groupId);
        secondaryProps.setProperty("max.poll.interval.ms", "300000");
        secondaryProps.setProperty("session.timeout.ms", "10000");
        secondaryProps.setProperty("heartbeat.interval.ms", "3000");
        secondaryProps.setProperty("auto.offset.reset", "latest");
        secondaryProps.setProperty("allow.auto.create.topics", "true");

        KafkaSource<String> secondarySource = KafkaSource.<String>builder()
                .setProperties(secondaryProps)
                .setTopics("zaOutPut")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.latest())
                .build();

        DataStream<String> secondaryStream = env.fromSource(
                secondarySource, WatermarkStrategy.noWatermarks(), "Secondary Kafka Source");

        // ================== 主数据处理 ==================
        SingleOutputStreamOperator<Tuple6<String, Integer, Long, List<Tuple4<Double, Double, Integer, Double>>, Integer, String>> primaryProcessed =
                primaryStream.flatMap(new PrimaryJSONParser())
                        .name("Primary Data Parser")
                        .setParallelism(3);

        // ================== 辅助数据处理 ==================
        SingleOutputStreamOperator<Tuple5<String, Integer, Long, List<Tuple4<Double, Double, Integer, Double>>,
                Integer>> secondaryProcessed =
                secondaryStream.flatMap(new SecondaryJSONParser())
                        .name("Secondary Data Parser")
                        .setParallelism(2);

        // ================== 输出到HBase ==================
        primaryProcessed.addSink(new PrimaryHBaseSink("ZCarTraj", "cf0"))
                .name("Primary HBase Sink")
                .setParallelism(2);

        secondaryProcessed.addSink(new SecondaryHBaseSink("ZZaCarTraj", "cf0"))
                .name("Secondary HBase Sink")
                .setParallelism(2);

        env.execute("Trajectory Storage Job (No Feature)");
    }

    // ================== 主数据解析器 ==================
    private static class PrimaryJSONParser implements FlatMapFunction<String,
            Tuple6<String, Integer, Long, List<Tuple4<Double, Double, Integer, Double>>, Integer, String>> {

        @Override
        public void flatMap(String jsonString,
                            Collector<Tuple6<String, Integer, Long, List<Tuple4<Double, Double, Integer, Double>>, Integer, String>> out) {

            try {
                JSONObject jsonObject = new JSONObject(jsonString);
                String timeSeg = jsonObject.getString("timeSeg");
                int type = jsonObject.getInt("type");
                long latestTime = jsonObject.getLong("latestTime");
                JSONArray trajectoryArray = jsonObject.getJSONArray("trajectory");
                String eventList = jsonObject.getJSONArray("eventList").toString();

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
                out.collect(new Tuple6<>(timeSeg, type, latestTime, trajectory, dir, eventList));
            } catch (JSONException e) {
                System.err.println("主数据解析失败: " + e.getMessage());
                System.err.println("原始数据: " + jsonString);
            }
        }
    }

    // ================== 辅助数据解析器 ==================
    private static class SecondaryJSONParser implements FlatMapFunction<String,
            Tuple5<String, Integer, Long, List<Tuple4<Double, Double, Integer, Double>>, Integer>> {

        @Override
        public void flatMap(String jsonString,
                            Collector<Tuple5<String, Integer, Long, List<Tuple4<Double, Double, Integer, Double>>, Integer>> out) {

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
                System.err.println("辅助数据解析失败: " + e.getMessage());
                System.err.println("原始数据: " + jsonString);
            }
        }
    }

    // ================== 主数据HBase Sink ==================
    private static class PrimaryHBaseSink extends RichSinkFunction<Tuple6<String, Integer, Long,
            List<Tuple4<Double, Double, Integer, Double>>, Integer, String>> {

        private final String baseTableName;
        private final String columnFamily;
        private transient Connection connection;
        private transient Table currentTable;
        private transient String currentTableName;
        private final ReentrantLock tableLock = new ReentrantLock();
        private static final ConcurrentHashMap<String, Object> tableCreationLocks = new ConcurrentHashMap<>();

        public PrimaryHBaseSink(String baseTableName, String columnFamily) {
            this.baseTableName = baseTableName;
            this.columnFamily = columnFamily;
        }

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            super.open(parameters);
            Configuration conf = createHBaseConfig();
            connection = ConnectionFactory.createConnection(conf);
        }

        @Override
        public void invoke(Tuple6<String, Integer, Long, List<Tuple4<Double, Double, Integer, Double>>,
                Integer, String> value, Context context) throws Exception {

            tableLock.lock();
            try {
                if(value.f3.size()<=2)return;

                String rowKey = value.f0;
                long rowKeyTime = parseRowKeyTime(rowKey);

                switchTableIfNeeded(rowKeyTime);

                Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("type"), Bytes.toBytes(value.f1.toString()));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("event_list"), Bytes.toBytes(value.f5));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("latest_time"), Bytes.toBytes(value.f2.toString()));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("trajectory"), Bytes.toBytes(value.f3.toString()));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("direction"), Bytes.toBytes(value.f4.toString()));

                currentTable.put(put);
            } catch (Exception e) {
                System.err.println("主数据HBase写入失败: " + e.getMessage());
                resetConnection();
            } finally {
                tableLock.unlock();
            }
        }

        private long parseRowKeyTime(String rowKey) {
            try {
                return Long.parseLong(rowKey.split("-")[0]);
            } catch (NumberFormatException e) {
                System.err.println("无效的主数据rowKey格式: " + rowKey);
                return System.currentTimeMillis();
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
                        System.out.println("主数据切换到HBase表: " + currentTableName);
                    }
                } finally {
                    tableLock.unlock();
                }
            }
        }

        private void createTableIfNotExists(String tableName) throws IOException {
            Object lock = tableCreationLocks.computeIfAbsent(tableName, k -> new Object());
            synchronized (lock) {
                try (Admin admin = connection.getAdmin()) {
                    TableName tn = TableName.valueOf(tableName);
                    if (!admin.tableExists(tn)) {
                        HTableDescriptor tableDescriptor = new HTableDescriptor(tn);
                        HColumnDescriptor cfDesc = new HColumnDescriptor(columnFamily);
                        tableDescriptor.addFamily(cfDesc);
                        admin.createTable(tableDescriptor);
                        System.out.println("创建主数据HBase表: " + tableName);
                    }
                }
            }
        }

        private void resetConnection() {
            try {
                if (connection != null) connection.close();
                Configuration conf = createHBaseConfig();
                connection = ConnectionFactory.createConnection(conf);
                if (currentTableName != null) {
                    currentTable = connection.getTable(TableName.valueOf(currentTableName));
                }
            } catch (IOException ex) {
                System.err.println("重建HBase连接失败: " + ex.getMessage());
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

    // ================== 辅助数据HBase Sink ==================
    private static class SecondaryHBaseSink extends RichSinkFunction<Tuple5<String, Integer, Long,
            List<Tuple4<Double, Double, Integer, Double>>, Integer>> {

        private final String baseTableName;
        private final String columnFamily;
        private transient Connection connection;
        private transient Table currentTable;
        private transient String currentTableName;
        private final ReentrantLock tableLock = new ReentrantLock();
        private static final ConcurrentHashMap<String, Object> tableCreationLocks = new ConcurrentHashMap<>();

        public SecondaryHBaseSink(String baseTableName, String columnFamily) {
            this.baseTableName = baseTableName;
            this.columnFamily = columnFamily;
        }

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            super.open(parameters);
            Configuration conf = createHBaseConfig();
            connection = ConnectionFactory.createConnection(conf);
        }

        @Override
        public void invoke(Tuple5<String, Integer, Long, List<Tuple4<Double, Double, Integer, Double>>,
                Integer> value, Context context) throws Exception {

            tableLock.lock();
            try {
                if(value.f3.size()<=2)return;
                String rowKey = value.f0;
                long rowKeyTime = parseRowKeyTime(rowKey);

                switchTableIfNeeded(rowKeyTime);

                Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("type"), Bytes.toBytes(value.f1.toString()));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("latest_time"), Bytes.toBytes(value.f2.toString()));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("trajectory"), Bytes.toBytes(value.f3.toString()));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("direction"), Bytes.toBytes(value.f4.toString()));

                currentTable.put(put);
            } catch (Exception e) {
                System.err.println("辅助数据HBase写入失败: " + e.getMessage());
                resetConnection();
            } finally {
                tableLock.unlock();
            }
        }

        private long parseRowKeyTime(String rowKey) {
            try {
                return Long.parseLong(rowKey.split("-")[0]);
            } catch (NumberFormatException e) {
                System.err.println("无效的辅助数据rowKey格式: " + rowKey);
                return System.currentTimeMillis();
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
                        System.out.println("辅助数据切换到HBase表: " + currentTableName);
                    }
                } finally {
                    tableLock.unlock();
                }
            }
        }

        private void createTableIfNotExists(String tableName) throws IOException {
            Object lock = tableCreationLocks.computeIfAbsent(tableName, k -> new Object());
            synchronized (lock) {
                try (Admin admin = connection.getAdmin()) {
                    TableName tn = TableName.valueOf(tableName);
                    if (!admin.tableExists(tn)) {
                        HTableDescriptor tableDescriptor = new HTableDescriptor(tn);
                        HColumnDescriptor cfDesc = new HColumnDescriptor(columnFamily);
                        tableDescriptor.addFamily(cfDesc);
                        admin.createTable(tableDescriptor);
                        System.out.println("创建辅助数据HBase表: " + tableName);
                    }
                }
            }
        }

        private void resetConnection() {
            try {
                if (connection != null) connection.close();
                Configuration conf = createHBaseConfig();
                connection = ConnectionFactory.createConnection(conf);
                if (currentTableName != null) {
                    currentTable = connection.getTable(TableName.valueOf(currentTableName));
                }
            } catch (IOException ex) {
                System.err.println("重建HBase连接失败: " + ex.getMessage());
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

    // ================== 公共配置方法 ==================
    private static Configuration createHBaseConfig() {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142,10.48.53.80");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("zookeeper.session.timeout", "120000");
        conf.set("hbase.rpc.timeout", "300000");
        conf.set("fs.defaultFS", "hdfs://100.65.38.139:9000");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        return conf;
    }
}