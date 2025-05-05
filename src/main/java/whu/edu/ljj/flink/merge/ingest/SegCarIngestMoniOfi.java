package whu.edu.ljj.flink.merge.ingest;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
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

public class SegCarIngestMoniOfi {
    public static void main(String[] args) throws Exception {
        // 设置 Flink 流执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(16);


        // 配置 KafkaSource
        String brokers = "100.65.38.40:9092";
        String groupId = "flink-group"; // 消费者组ID
        String topic = "MergedPathData";


        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 从 KafkaSource 创建数据流
        DataStreamSource<String> kafkaStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source"
        );

        // 保存 flatMap 操作后的结果
        DataStream<Tuple2<String, String>> flatMapStream = kafkaStream
                .flatMap(new FlatMapFunction<String, Tuple2<String, String>>() {
                    @Override
                    public void flatMap(String jsonString, Collector<Tuple2<String, String>> out) {
                        try {
                            JSONObject jsonObject = new JSONObject(jsonString);
                            String timeStampStr = jsonObject.getString("timeStamp");
                            long timeObs;
                            try {
                                // 尝试按三位毫秒格式解析
                                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS");
                                LocalDateTime localDateTime = LocalDateTime.parse(timeStampStr, formatter);
                                timeObs = localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
                            } catch (Exception e) {
                                // 若三位毫秒格式解析失败，尝试按两位毫秒格式解析
                                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SS");
                                LocalDateTime localDateTime = LocalDateTime.parse(timeStampStr, formatter);
                                timeObs = localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
                            }

                            // 截取到分钟级的时间戳
                            long minuteTimestamp = timeObs / 60000 * 60000;

                            JSONArray tdataArray = jsonObject.getJSONArray("pathList");
                            for (int i = 0; i < tdataArray.length(); i++) {
                                JSONObject tdataObject = tdataArray.getJSONObject(i);
                                String carNumber = tdataObject.getString("plateNo");
                                double mileage;
                                try {
                                    mileage = tdataObject.getInt("mileage");
                                } catch (JSONException e) {
                                    // 如果找不到direction字段，给予默认值
                                    mileage = -1000;
                                }
//                                int mileage = tdataObject.getInt("mileage");
                                // 截取到公里级别
                                double mileageKm = Math.floor(mileage / 1000);

                                // 构造rowkey：时间戳_里程号
                                String rowkey = minuteTimestamp + "_" + mileageKm;
                                out.collect(new Tuple2<>(rowkey, carNumber));
                            }
                        } catch (Exception e) {
                            System.err.println("解析 JSON 时出错: " + e.getMessage());
                        }
                    }
                });

        // 按 rowkey 分组并处理
        DataStream<Tuple2<String, String>> processedStream = flatMapStream.keyBy(value -> value.f0)
                .process(new KeyedProcessFunction<String, Tuple2<String, String>, Tuple2<String, String>>() {
                    private transient Set<String> carNumbers;
                    //carnumber: speed,direction,n
                    private long timerInterval = 60 * 1000; // 60 秒
                    private boolean timerRegistered = false;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        carNumbers = new HashSet<>();
                    }

                    @Override
                    public void processElement(Tuple2<String, String> value, Context ctx, Collector<Tuple2<String, String>> out) throws Exception {
                        carNumbers.add(value.f1);
                        if (!timerRegistered) {
                            long currentTime = ctx.timerService().currentProcessingTime();
                            long triggerTime = currentTime + timerInterval;
                            ctx.timerService().registerProcessingTimeTimer(triggerTime);
                            timerRegistered = true;
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, String>> out) throws Exception {
                        if (!carNumbers.isEmpty()) {
                            String mergedCarNumbers = String.join(",", carNumbers);
                            out.collect(new Tuple2<>(ctx.getCurrentKey(), mergedCarNumbers));
                            // 清空集合
                            carNumbers.clear();
                        }
                        timerRegistered = false;
                    }
                }) .returns(Types.TUPLE(Types.STRING, Types.STRING)); // 显式指定输出类型;

//                .addSink(new SinkFunction<Tuple2<String, String>>() {
//                    @Override
//                    public void invoke(Tuple2<String, String> value, Context context) {
//                        System.out.println("RowKey: " + value.f0 + ", Car Numbers: " + value.f1);
//                    }
//                });

        // 打印调试（非终端操作）
//        processedStream.map(data -> {
//            System.out.println("Before Sink Data: RowKey=" + data.f0 + ", Cars=" + data.f1);
//            return data;
//        }).returns(Types.TUPLE(Types.STRING, Types.STRING)); // 显式指定输出类型

// 添加 Sink（终端操作）
        processedStream.addSink(new DynamicHBaseSink("STCar", "cf", 60));

        env.execute("Flink STCar to HBase");
    }

    // HBase Sink 实现
    public static class DynamicHBaseSink extends RichSinkFunction<Tuple2<String, String>> {
        private final String baseTableName;
        private final String columnFamily;
        private final long intervalSeconds; // 表切换间隔（秒）

        private transient org.apache.hadoop.conf.Configuration hadoopConf;
        private transient Connection hbaseConnection;
        private transient Table hbaseTable;

        private transient String currentTableName;
        private transient LocalDateTime nextTableSwitchTime;
        private final ReentrantLock tableLock = new ReentrantLock();
        private static final ConcurrentHashMap<String, Object> tableCreationLocks = new ConcurrentHashMap<>();

        public DynamicHBaseSink(String baseTableName, String columnFamily, long intervalSeconds) {
            this.baseTableName = baseTableName;
            this.columnFamily = columnFamily;
            this.intervalSeconds = intervalSeconds;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            hadoopConf = HBaseConfiguration.create();
            hadoopConf.set("hbase.zookeeper.quorum", "100.65.38.139,100.65.38.140,100.65.38.141");
            hadoopConf.set("hbase.zookeeper.property.clientPort", "2181");
            hadoopConf.set("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", "400");
            hadoopConf.set("fs.defaultFS", "hdfs://100.65.38.139:9000");
            hadoopConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            hbaseConnection = ConnectionFactory.createConnection(hadoopConf);

            currentTableName = null;
            nextTableSwitchTime = null;
        }

        @Override
        public void invoke(Tuple2<String, String> value, Context context) throws Exception {
            tableLock.lock();
            try {
                long rowKeyTime = Long.parseLong(value.f0.split("_")[0]);
//                System.out.println("Sink Invoke - RowKey: " + value.f0 + ", Car Numbers: " + value.f1);

                if (currentTableName == null || isTimeToSwitch(rowKeyTime)) {
                    switchTable(rowKeyTime);
                }

                String rowKey = value.f0;
                Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("car_numbers"), Bytes.toBytes(value.f1.toString()));
                hbaseTable.put(put);
            } finally {
                tableLock.unlock();
            }
        }

        @Override
        public void close() throws Exception {
            if (hbaseTable != null) {
                hbaseTable.close();
            }
            if (hbaseConnection != null) {
                hbaseConnection.close();
            }
        }

        private boolean isTimeToSwitch(long rowKeyTime) {
            Instant instant = Instant.ofEpochMilli(rowKeyTime);
            LocalDateTime rowKeyDateTime = instant.atZone(ZoneId.systemDefault()).toLocalDateTime();
            return rowKeyDateTime.isAfter(nextTableSwitchTime);
        }


        private void switchTable(long rowKeyTime) throws Exception {
            tableLock.lock();
            try {
                Instant instant = Instant.ofEpochMilli(rowKeyTime);
                LocalDateTime rowKeyDateTime = instant.atZone(ZoneId.systemDefault()).toLocalDateTime();

                if (nextTableSwitchTime == null) {
                    nextTableSwitchTime = rowKeyDateTime.plusSeconds(intervalSeconds);
                }

                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd_HHmm");
                String timestamp = rowKeyDateTime.format(formatter);
                currentTableName = baseTableName + "_" + timestamp;

                createTableIfNotExists(currentTableName, columnFamily);

                if (hbaseTable != null) {
                    hbaseTable.close();
                }
                hbaseTable = hbaseConnection.getTable(TableName.valueOf(currentTableName));

                nextTableSwitchTime = rowKeyDateTime.plusSeconds(intervalSeconds);

                System.out.printf("切换到新表: %s，下一次切换时间: %s%n", currentTableName, nextTableSwitchTime.format(formatter));
            } finally {
                tableLock.unlock();
            }
        }

        private void createTableIfNotExists(String tableName, String columnFamily) {
            tableLock.lock();
            try (Admin admin = hbaseConnection.getAdmin()) {
                TableName hbaseTableName = TableName.valueOf(tableName);

                Object lock = tableCreationLocks.computeIfAbsent(tableName, k -> new Object());

                synchronized (lock) {
                    // 获取最新的表列表
                    admin.listTables();
                    if (!admin.tableExists(hbaseTableName)) {
                        HTableDescriptor tableDescriptor = new HTableDescriptor(hbaseTableName);
                        tableDescriptor.addFamily(new HColumnDescriptor(columnFamily));
                        try {
                            admin.createTable(tableDescriptor);
                            System.out.println("Table created: " + tableName);
                        } catch (TableExistsException e) {
                            System.out.println("Table already exists, but not detected by tableExists(): " + tableName);
                        }
                    } else {
                        System.out.println("Table already exists: " + tableName);
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                tableLock.unlock();
            }
        }

//        private void createTableIfNotExists(String tableName, String columnFamily) throws Exception {
//            tableLock.lock();
//            try (Admin admin = hbaseConnection.getAdmin()) {
//                TableName hbaseTableName = TableName.valueOf(tableName);
//
//                Object lock = tableCreationLocks.computeIfAbsent(tableName, k -> new Object());
//
//                synchronized (lock) {
//                    if (!admin.tableExists(hbaseTableName)) {
//                        HTableDescriptor tableDescriptor = new HTableDescriptor(hbaseTableName);
//                        tableDescriptor.addFamily(new HColumnDescriptor(columnFamily));
//                        admin.createTable(tableDescriptor);
//                        System.out.println("Table created: " + tableName);
//                    } else {
//                        System.out.println("Table already exists: " + tableName);
//                    }
//                }
//            } finally {
//                tableLock.unlock();
//            }
//        }
    }
}