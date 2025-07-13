package whu.edu.moniData.ingest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DailyTrafficCounter {

    private static final String DAILY_TRAFFIC_TABLE = "daily_vehicle_count";
    private static final String ZK_QUORUM = "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142,10.48.53.80";
    private static final String ZK_PORT = "2181";

    public static void main(String[] args) {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        // 计算离下个零点的时间差（秒）
        long initialDelay = calculateSecondsToNextMidnight();

        // 每天零点执行统计任务
        scheduler.scheduleAtFixedRate(
                () -> {
                    try (Connection conn = getHBaseConnection()) {
                        LocalDate yesterday = LocalDate.now().minusDays(1);

                        // 统计上/下行流量
                        int upCount = countByDirection(conn, yesterday, 1);
                        int downCount = countByDirection(conn, yesterday, 2);

                        // 保存结果
                        saveTrafficStats(conn, yesterday, upCount, downCount);

                        System.out.println("统计完成: " + yesterday +
                                " => 上行: " + upCount + ", 下行: " + downCount);
                    } catch (Exception e) {
                        System.err.println("统计任务失败: " + e.getMessage());
                        e.printStackTrace();
                    }
                },
                initialDelay,
                24 * 60 * 60,  // 24小时
                TimeUnit.SECONDS
        );
    }

    /**
     * 统计某日指定方向的车辆数量
     */
    private static int countByDirection(Connection conn, LocalDate date, int direction) throws IOException {
        String tableName = "ZCarTraj_" + date.format(DateTimeFormatter.BASIC_ISO_DATE);

        // 确保表存在
        try (Admin admin = conn.getAdmin()) {
            if (!admin.tableExists(TableName.valueOf(tableName))) {
                System.out.println("表不存在: " + tableName);
                return 0;
            }
        }

        try (Table table = conn.getTable(TableName.valueOf(tableName))) {
            // 创建过滤器：匹配指定方向
            SingleColumnValueFilter directionFilter = new SingleColumnValueFilter(
                    Bytes.toBytes("cf0"),
                    Bytes.toBytes("direction"),
                    CompareFilter.CompareOp.EQUAL,
                    new BinaryComparator(Bytes.toBytes(String.valueOf(direction)))
            );

            // 计数器
            int count = 0;
            Scan scan = new Scan()
                    .setFilter(directionFilter)
                    .setCaching(1000)        // 批量获取
                    .setCacheBlocks(false);  // 不缓存扫描块

            try (ResultScanner scanner = table.getScanner(scan)) {
                // 只需统计行数，无需处理内容
                for (Result result : scanner) {
                    count++;
                }
            }
            return count;
        }
    }

    /**
     * 保存统计结果到HBase
     */
    private static void saveTrafficStats(Connection conn, LocalDate date, int upCount, int downCount)
            throws IOException {
        String rowKey = date.format(DateTimeFormatter.BASIC_ISO_DATE);

        try (Table table = conn.getTable(TableName.valueOf(DAILY_TRAFFIC_TABLE))) {
            Put put = new Put(Bytes.toBytes(rowKey));

            // 保存上行流量
            put.addColumn(
                    Bytes.toBytes("stats"),
                    Bytes.toBytes("up"),
                    Bytes.toBytes(upCount)  // 整数值
            );

            // 保存下行流量
            put.addColumn(
                    Bytes.toBytes("stats"),
                    Bytes.toBytes("down"),
                    Bytes.toBytes(downCount)  // 整数值
            );

            table.put(put);
        }
    }

    /**
     * 获取HBase连接
     */
    private static Connection getHBaseConnection() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", ZK_QUORUM);
        conf.set("hbase.zookeeper.property.clientPort", ZK_PORT);
        return ConnectionFactory.createConnection(conf);
    }

    /**
     * 计算距离下个零点的时间（秒）
     */
    private static long calculateSecondsToNextMidnight() {
        ZonedDateTime now = ZonedDateTime.now(ZoneId.systemDefault());
        ZonedDateTime nextMidnight = now.plusDays(1).withHour(0).withMinute(0).withSecond(0);
        return Duration.between(now, nextMidnight).getSeconds();
    }

    /**
     * 测试方法
     */
    public static void testManualCount() {
        try (Connection conn = getHBaseConnection()) {
            // 手动测试昨天的统计
            LocalDate testDate = LocalDate.now().minusDays(1);

            int upCount = countByDirection(conn, testDate, 1);
            int downCount = countByDirection(conn, testDate, 2);

            System.out.println("========== 流量统计测试 ==========");
            System.out.println("日期: " + testDate);
            System.out.println("上行车辆: " + upCount);
            System.out.println("下行车辆: " + downCount);
            System.out.println("总车流量: " + (upCount + downCount));
            System.out.println("===============================");
        } catch (Exception e) {
            System.err.println("测试失败: " + e.getMessage());
            e.printStackTrace();
        }
    }
}