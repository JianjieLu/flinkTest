package whu.edu.moniData.ingest.newIngest;


import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class TrafficIndicatorHBaseSink extends RichSinkFunction<TrafficMetricsCalculationAndStorageJob.IndicatorResult> {

    private final String baseTableName;
    private final String columnFamily;
    private transient Connection connection;
    private transient Table currentTable;
    private transient String currentTableName;
    private final ReentrantLock tableLock = new ReentrantLock();
    private static final ConcurrentHashMap<String, Object> tableCreationLocks = new ConcurrentHashMap<>();

    public TrafficIndicatorHBaseSink(String baseTableName, String columnFamily) {
        this.baseTableName = baseTableName;
        this.columnFamily = columnFamily;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        org.apache.hadoop.conf.Configuration conf = createHBaseConfig();
        connection = ConnectionFactory.createConnection(conf);
    }

    @Override
    public void invoke(TrafficMetricsCalculationAndStorageJob.IndicatorResult indicator, Context context) throws Exception {
        tableLock.lock();
        try {
            // 生成行键: 时间键_时间类型_车道_方向_桩号
            String rowKey = String.format("%s_%s_%d_%d_%d",
                    indicator.getTimeKey(),
                    indicator.getTimeType(),
                    indicator.getLaneNo(),
                    indicator.getDirection(),
                    indicator.getStake());

            // 获取时间戳用于分表
            long timestamp = parseTimeKey(indicator.getTimeKey(), indicator.getTimeType());

            switchTableIfNeeded(timestamp);

            Put put = new Put(Bytes.toBytes(rowKey));

            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("occupancy"), Bytes.toBytes(String.valueOf(indicator.getOccupancy())));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("headway"), Bytes.toBytes(String.valueOf(indicator.getHeadway())));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("delay_index"), Bytes.toBytes(String.valueOf(indicator.getDelayIndex())));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("vehicle_count"), Bytes.toBytes(String.valueOf(indicator.getVehicleCount())));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("update_time"), Bytes.toBytes(String.valueOf(System.currentTimeMillis())));

            currentTable.put(put);

            System.out.println("Stored indicator to HBase: " + rowKey);
        } catch (Exception e) {
            System.err.println("HBase写入失败: " + e.getMessage());
            resetConnection();
        } finally {
            tableLock.unlock();
        }
    }

    private long parseTimeKey(String timeKey, String timeType) {
        try {
            DateTimeFormatter formatter;
            switch (timeType) {
                case "minute":
                    formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmm");
                    break;
                case "hour":
                    formatter = DateTimeFormatter.ofPattern("yyyyMMddHH");
                    break;
                case "day":
                    formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
                    break;
                case "month":
                    formatter = DateTimeFormatter.ofPattern("yyyyMM");
                    break;
                default:
                    return System.currentTimeMillis();
            }

            LocalDateTime dateTime = LocalDateTime.parse(timeKey, formatter);
            return dateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        } catch (Exception e) {
            System.err.println("解析时间键失败: " + timeKey + ", type: " + timeType);
            return System.currentTimeMillis();
        }
    }

    private void switchTableIfNeeded(long timestamp) throws IOException {
        LocalDateTime dateTime = LocalDateTime.ofInstant(
                java.time.Instant.ofEpochMilli(timestamp), ZoneId.systemDefault()
        );
        String newTableName = baseTableName + "_" + dateTime.format(DateTimeFormatter.BASIC_ISO_DATE);

        if (currentTable == null || !newTableName.equals(currentTableName)) {
            tableLock.lock();
            try {
                if (currentTable == null || !newTableName.equals(currentTableName)) {
                    createTableIfNotExists(newTableName);
                    if (currentTable != null) currentTable.close();
                    currentTable = connection.getTable(TableName.valueOf(newTableName));
                    currentTableName = newTableName;
                    System.out.println("切换到HBase表: " + currentTableName);
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
                    System.out.println("创建HBase表: " + tableName);
                }
            }
        }
    }

    private void resetConnection() {
        try {
            if (connection != null) connection.close();
            org.apache.hadoop.conf.Configuration conf = createHBaseConfig();
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

    private org.apache.hadoop.conf.Configuration createHBaseConfig() {
        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142,10.48.53.80");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("zookeeper.session.timeout", "120000");
        conf.set("hbase.rpc.timeout", "300000");
        conf.set("fs.defaultFS", "hdfs://100.65.38.139:9000");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        return conf;
    }
}