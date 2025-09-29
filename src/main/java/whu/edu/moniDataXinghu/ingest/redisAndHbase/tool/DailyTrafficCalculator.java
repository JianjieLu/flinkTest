package whu.edu.moniDataXinghu.ingest.redisAndHbase.tool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class DailyTrafficCalculator {

    // HBase配置
    private static Configuration createHBaseConfig() {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.0.5,192.168.0.7,192.168.0.8:,192.168.0.9,192.168.0.11,192.168.0.12");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("zookeeper.session.timeout", "120000");
        conf.set("hbase.rpc.timeout", "300000");
        conf.set("fs.defaultFS", "hdfs://192.168.0.5:9000");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        return conf;
    }

    // 获取所有相关的表名
    public static List<String> getTrajectoryTables(Admin admin, String baseTableName,
                                                   String startDate, String endDate) throws IOException {
        List<String> tableNames = new ArrayList<>();
        TableName[] tables = admin.listTableNames();

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");

        for (TableName table : tables) {
            String tableNameStr = table.getNameAsString();
            if (tableNameStr.startsWith(baseTableName + "_")) {
                // 提取表名中的日期部分
                String datePart = tableNameStr.substring(baseTableName.length() + 1);

                // 检查日期是否在指定范围内
                if (isDateInRange(datePart, startDate, endDate, sdf)) {
                    tableNames.add(tableNameStr);
                }
            }
        }
        return tableNames;
    }

    // 检查日期是否在指定范围内
    private static boolean isDateInRange(String dateStr, String startDate, String endDate, SimpleDateFormat sdf) {
        try {
            Date date = sdf.parse(dateStr);
            Date start = startDate != null ? sdf.parse(startDate) : null;
            Date end = endDate != null ? sdf.parse(endDate) : null;

            if (start != null && date.before(start)) {
                return false;
            }

            if (end != null && date.after(end)) {
                return false;
            }

            return true;
        } catch (ParseException e) {
            System.err.println("日期解析错误: " + dateStr + " - " + e.getMessage());
            return false;
        }
    }

    // 计算单表的行数（车流量）
    public static long countTableRows(Table table) throws IOException {
        long rowCount = 0;
        Scan scan = new Scan();
        scan.setCaching(1000); // 设置每次扫描的行数，优化性能

        try (ResultScanner scanner = table.getScanner(scan)) {
            for (Result result : scanner) {
                rowCount++;
            }
        }
        return rowCount;
    }

    // 创建车流量统计表
    public static void createTrafficTableIfNotExists(Admin admin, String tableName) throws IOException {
        TableName tn = TableName.valueOf(tableName);
        if (!admin.tableExists(tn)) {
            HTableDescriptor tableDescriptor = new HTableDescriptor(tn);
            HColumnDescriptor cfDesc = new HColumnDescriptor("cf0");
            tableDescriptor.addFamily(cfDesc);
            admin.createTable(tableDescriptor);
            System.out.println("创建车流量统计表: " + tableName);
        }
    }

    // 存储车流量数据到HBase
    public static void storeDailyTraffic(Connection connection, String tableName,
                                         Map<String, Long> dailyTraffic) throws IOException {

        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            for (Map.Entry<String, Long> entry : dailyTraffic.entrySet()) {
                String date = entry.getKey();
                long trafficCount = entry.getValue();

                Put put = new Put(Bytes.toBytes(date));
                put.addColumn(Bytes.toBytes("cf0"), Bytes.toBytes("traffic_count"),
                        Bytes.toBytes(String.valueOf(trafficCount)));

                table.put(put);
                System.out.println("存储车流量数据: " + date + " -> " + trafficCount);
            }
        }
    }

    // 解析命令行参数
    private static Map<String, String> parseArguments(String[] args) {
        Map<String, String> params = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            if (args[i].startsWith("--")) {
                String key = args[i].substring(2);
                if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
                    params.put(key, args[i + 1]);
                    i++; // 跳过值
                } else {
                    params.put(key, null);
                }
            }
        }
        return params;
    }

    // 主执行方法
    public static void main(String[] args) {
        Connection connection = null;
        Admin admin = null;

        try {
            // 解析命令行参数
            Map<String, String> params = parseArguments(args);
            String startDate = params.get("startDate");
            String endDate = params.get("endDate");
            String days = params.get("days");

            // 如果没有指定日期范围，使用默认值
            if (startDate == null && endDate == null && days != null) {
                int numDays = Integer.parseInt(days);
                Calendar calendar = Calendar.getInstance();
                SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
                endDate = sdf.format(calendar.getTime());
                calendar.add(Calendar.DAY_OF_YEAR, -numDays);
                startDate = sdf.format(calendar.getTime());
            }

            System.out.println("开始日期: " + (startDate != null ? startDate : "无限制"));
            System.out.println("结束日期: " + (endDate != null ? endDate : "无限制"));

            Configuration conf = createHBaseConfig();
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();

            // 需要统计的表名前缀
            String[] baseTableNames = {"ZCarTraj", "ZZaCarTraj"};
            String resultTableName = "DailyTraffic";

            // 创建结果表
            createTrafficTableIfNotExists(admin, resultTableName);

            // 存储每日车流量的映射
            Map<String, Long> dailyTraffic = new ConcurrentHashMap<>();

            // 遍历所有需要统计的表
            for (String baseTableName : baseTableNames) {
                List<String> tableNames = getTrajectoryTables(admin, baseTableName, startDate, endDate);
                System.out.println("找到 " + tableNames.size() + " 个表需要处理 (" + baseTableName + ")");

                for (String tableName : tableNames) {
                    try (Table table = connection.getTable(TableName.valueOf(tableName))) {
                        // 从表名中提取日期（假设表名格式为baseTableName_yyyyMMdd）
                        String dateStr = tableName.substring(baseTableName.length() + 1);

                        // 计算该表的行数（车流量）
                        long rowCount = countTableRows(table);

                        // 累加到对应日期的车流量
                        dailyTraffic.merge(dateStr, rowCount, Long::sum);

                        System.out.println("表 " + tableName + " 的车流量: " + rowCount);
                    } catch (Exception e) {
                        System.err.println("处理表 " + tableName + " 时出错: " + e.getMessage());
                    }
                }
            }

            // 存储车流量统计数据
            storeDailyTraffic(connection, resultTableName, dailyTraffic);

            System.out.println("车流量统计完成，共统计 " + dailyTraffic.size() + " 天的数据");

        } catch (Exception e) {
            System.err.println("车流量统计失败: " + e.getMessage());
            e.printStackTrace();
        } finally {
            try {
                if (admin != null) admin.close();
                if (connection != null) connection.close();
            } catch (IOException e) {
                System.err.println("关闭连接时出错: " + e.getMessage());
            }
        }
    }
}