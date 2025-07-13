package whu.edu.moniData.ingest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class HourlyTrafficFromTrajData {

    private static final String HOURLY_FLOW_TABLE = "";
    private static final String COLUMN_FAMILY = "cf";
    private static final String ZOOKEEPER_QUORUM = "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142,10.48.53.80";

    public static void main(String[] args) throws IOException {
        // 确保目标表存在
        ensureHourlyFlowTableExists();

        // 创建调度线程池
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        // 计算首次执行的延迟时间（等待到下一个整点）
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime nextHour = now.plusHours(1).withMinute(0).withSecond(0).withNano(0);
        long initialDelay = Duration.between(now, nextHour).getSeconds();
        System.out.println("延迟：" + initialDelay);

        // 每1小时执行一次
//        scheduler.scheduleAtFixedRate(
//                () -> {
//                    try {
                        processHourlyTraffic();
//                    } catch (Exception e) {
//                        System.err.println("处理小时流量出错: " + e.getMessage());
//                        e.printStackTrace();
//                    }
//                },
//                initialDelay,
//                20,  // 1小时 = 3600秒
//                TimeUnit.SECONDS
//        );
    }

    private static void ensureHourlyFlowTableExists() {
        try {
            Configuration conf = getHBaseConfig();
            try (Connection conn = ConnectionFactory.createConnection(conf);
                 Admin admin = conn.getAdmin()) {

                TableName tableName = TableName.valueOf(HOURLY_FLOW_TABLE);

                if (!admin.tableExists(tableName)) {
                    // 创建表结构
                    HTableDescriptor tableDesc = new HTableDescriptor(tableName);
                    HColumnDescriptor columnFamily = new HColumnDescriptor(COLUMN_FAMILY);
                    columnFamily.setMaxVersions(1); // 只保留一个版本
                    tableDesc.addFamily(columnFamily);

                    // 执行建表
                    admin.createTable(tableDesc);
                    System.out.println("成功创建表: " + HOURLY_FLOW_TABLE);
                } else {
                    System.out.println("表已存在: " + HOURLY_FLOW_TABLE);
                }
            }
        } catch (IOException e) {
            System.err.println("无法创建表 " + HOURLY_FLOW_TABLE + ": " + e.getMessage());
        }
    }

    private static void processHourlyTraffic() throws IOException {
        // 1. 获取上一小时的开始时间
        ZonedDateTime previousHour = ZonedDateTime.now(ZoneId.systemDefault())
                .minusHours(1)
                .withMinute(0)
                .withSecond(0)
                .withNano(0);

        // 2. 创建行键 (格式: yyyyMMddHH)
        String rowKey = previousHour.format(DateTimeFormatter.ofPattern("yyyyMMddHH"));

        // 3. 查询当前小时的车流量
        int count = getHourlyTrafficCount(previousHour);
        System.out.println("小时车流量: " + count);
        long executionTime = System.currentTimeMillis();

        // 4. 存储结果到hourlyFlow表
        storeHourlyFlow(rowKey, count, executionTime);

        System.out.printf("[%s] 存储小时流量: %d 条%n",
                LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
                count);
    }

    private static int getHourlyTrafficCount(ZonedDateTime hour) throws IOException {
        Configuration conf = getHBaseConfig();

        try (Connection conn = ConnectionFactory.createConnection(conf)) {
            // 修复表名格式问题：使用yyyyMMdd格式，避免非法字符
            String tableName = "ZCarTraj_" + hour.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
            System.out.println("查询表: " + tableName);

            if (!isTableExists(conn, tableName)) {
                System.out.printf("跳过不存在的表: %s%n", tableName);
                return 0;
            }

            // 计算本小时时间范围 (毫秒)
            long startTime = hour.toInstant().toEpochMilli();
            long endTime = hour.plusHours(1).toInstant().toEpochMilli();

            System.out.printf("时间范围: %d - %d (%s - %s)%n",
                    startTime, endTime,
                    Instant.ofEpochMilli(startTime).atZone(ZoneId.systemDefault()),
                    Instant.ofEpochMilli(endTime).atZone(ZoneId.systemDefault()));

            // 创建扫描器（优化扫描范围）
            Scan scan = new Scan()
                    .setRowPrefixFilter(Bytes.toBytes(String.valueOf(startTime)))
                    .setCaching(1000); // 提高扫描性能

            // 扫描并计数
            try (Table table = conn.getTable(TableName.valueOf(tableName));
                 ResultScanner scanner = table.getScanner(scan)) {

                int count = 0;
                int totalScanned = 0;
                for (Result res : scanner) {
                    totalScanned++;
                    // 获取行键中的时间戳部分
                    String rowKeyStr = Bytes.toString(res.getRow());
                    String[] parts = rowKeyStr.split("-");
                    if (parts.length == 0) continue;

                    try {
                        long recordTime = Long.parseLong(parts[0]);
                        // 精确匹配时间范围
                        if (recordTime >= startTime && recordTime < endTime) {
                            count++;
                        }
                    } catch (NumberFormatException e) {
                        System.err.println("无效的行键格式: " + rowKeyStr);
                    }

                    // 每1000条记录打印一次进度
                    if (totalScanned % 1000 == 0) {
                        System.out.printf("已扫描 %d 条记录，当前计数: %d%n", totalScanned, count);
                    }
                }
                System.out.printf("扫描完成: 总计 %d 条，有效计数 %d%n", totalScanned, count);
                return count;
            }
        }
    }

    private static void storeHourlyFlow(String rowKey, int count, long execTime) throws IOException {
        Configuration conf = getHBaseConfig();

        try (Connection conn = ConnectionFactory.createConnection(conf);
             Table table = conn.getTable(TableName.valueOf(HOURLY_FLOW_TABLE))) {

            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(
                    Bytes.toBytes(COLUMN_FAMILY),
                    Bytes.toBytes("count"),
                    Bytes.toBytes(count)
            );
            put.addColumn(
                    Bytes.toBytes(COLUMN_FAMILY),
                    Bytes.toBytes("execution_time"),
                    Bytes.toBytes(execTime)
            );

            table.put(put);
            System.out.println("成功存储行键: " + rowKey);
        }
    }

    private static boolean isTableExists(Connection conn, String tableName) throws IOException {
        try (Admin admin = conn.getAdmin()) {
            return admin.tableExists(TableName.valueOf(tableName));
        }
    }

    private static Configuration getHBaseConfig() {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", ZOOKEEPER_QUORUM);
        conf.set("zookeeper.session.timeout", "120000");
        conf.set("hbase.client.scanner.timeout.period", "600000");
        conf.set("hbase.client.operation.timeout", "600000");
        return conf;
    }
}