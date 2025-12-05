package whu.edu.moniData.ingest.holyAnalysisJob.redisAndHbase.tool;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.time.*;
import java.util.*;

public class JTSTCarDataReplicatorV2 {

    private static Connection connection;
    private static final int BATCH_SIZE = 500; // 减小批量大小以避免超时

    static {
        try {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142");
            conf.set("hbase.zookeeper.property.clientPort", "2181");

            // 优化连接配置
            conf.set("hbase.client.write.buffer", "2097152"); // 2MB
            conf.set("hbase.client.max.perregion.tasks", "50");
            conf.set("hbase.rpc.timeout", "300000"); // 5分钟
            conf.set("hbase.client.operation.timeout", "300000"); // 5分钟
            conf.set("hbase.client.scanner.timeout.period", "300000");

            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 主方法：复制昨天数据到2023年全年
    public static void replicateYesterdayTo2023(String startDate) throws IOException {
        long startTime = System.currentTimeMillis();

        // 1. 获取昨天的日期
        LocalDate yesterday = LocalDate.now().minusDays(1);
        System.out.println("获取昨天数据，日期: " + yesterday);

        // 2. 获取昨天的季度表名
        String yesterdayQuarter = getQuarterTableName(yesterday);
        String sourceTableName = "JTSTCar_" + yesterdayQuarter;
        System.out.println("源表: " + sourceTableName);

        // 3. 检查源表是否存在
        if (!tableExists(sourceTableName)) {
            System.err.println("源表不存在: " + sourceTableName);
            System.err.println("可用的表:");
            listAllTables();
            return;
        }

        // 4. 获取昨天的所有分钟级数据
        List<VehicleMinuteData> yesterdayData = exportDayData(sourceTableName, yesterday);
        System.out.println("导出昨天数据: " + yesterdayData.size() + " 条分钟级记录");

        if (yesterdayData.isEmpty()) {
            System.out.println("警告: 昨天没有数据，无法复制");
            return;
        }

        // 5. 生成2023年从指定日期开始的所有日期
        List<LocalDate> dates2023 = generateDatesFromStartDate(2023, startDate);
        System.out.println("生成2023年从 " + startDate + " 开始的日期: " + dates2023.size() + " 天");

        // 6. 复制数据到2023年每一天
        replicateDataToDates(yesterdayData, dates2023);

        long endTime = System.currentTimeMillis();
        System.out.println("数据复制完成！耗时: " + (endTime - startTime) / 1000 + " 秒");
    }

    // 生成从指定日期开始的日期
    private static List<LocalDate> generateDatesFromStartDate(int year, String startDateStr) {
        List<LocalDate> dates = new ArrayList<>();

        // 解析开始日期
        int startYear = Integer.parseInt(startDateStr.substring(0, 4));
        int startMonth = Integer.parseInt(startDateStr.substring(4, 6));
        int startDay = Integer.parseInt(startDateStr.substring(6, 8));

        LocalDate startDate = LocalDate.of(startYear, startMonth, startDay);
        LocalDate endDate = LocalDate.of(year, 12, 31);

        LocalDate current = startDate;
        while (!current.isAfter(endDate)) {
            dates.add(current);
            current = current.plusDays(1);
        }

        return dates;
    }

    // 获取季度的表名
    private static String getQuarterTableName(LocalDate date) {
        int year = date.getYear();
        int month = date.getMonthValue();
        int quarter = (month - 1) / 3 + 1;
        return year + "Q" + quarter;
    }

    private static String getQuarterTableName(long timestampMs) {
        ZoneId zone = ZoneId.of("Asia/Shanghai");
        Instant instant = Instant.ofEpochMilli(timestampMs);
        LocalDateTime dt = LocalDateTime.ofInstant(instant, zone);

        int year = dt.getYear();
        int month = dt.getMonthValue();
        int quarter = (month - 1) / 3 + 1;

        return String.format("%dQ%d", year, quarter);
    }

    // 检查表是否存在
    private static boolean tableExists(String tableName) {
        try (Admin admin = connection.getAdmin()) {
            return admin.tableExists(TableName.valueOf(tableName));
        } catch (IOException e) {
            System.err.println("检查表是否存在时出错: " + e.getMessage());
            return false;
        }
    }

    // 列出所有表
    private static void listAllTables() {
        try (Admin admin = connection.getAdmin()) {
            TableName[] tableNames = admin.listTableNames();
            for (TableName tableName : tableNames) {
                System.out.println("  - " + tableName.getNameAsString());
            }
        } catch (IOException e) {
            System.err.println("列出表时出错: " + e.getMessage());
        }
    }

    // 创建表（如果不存在）
    private static void createTableIfNotExists(String tableName) throws IOException {
        try (Admin admin = connection.getAdmin()) {
            TableName hbaseTableName = TableName.valueOf(tableName);

            if (!admin.tableExists(hbaseTableName)) {
                System.out.println("创建表: " + tableName);

                HTableDescriptor tableDescriptor = new HTableDescriptor(hbaseTableName);
                HColumnDescriptor columnDescriptor = new HColumnDescriptor("cf");
                tableDescriptor.addFamily(columnDescriptor);

                // 设置合理的配置
                columnDescriptor.setMaxVersions(1);
                columnDescriptor.setMinVersions(1);
                columnDescriptor.setTimeToLive(31536000); // 1年

                admin.createTable(tableDescriptor);
                System.out.println("表创建成功: " + tableName);
            }
        }
    }

    // 导出指定日期的所有分钟数据
    private static List<VehicleMinuteData> exportDayData(String sourceTableName, LocalDate date) throws IOException {
        List<VehicleMinuteData> result = new ArrayList<>();

        if (!tableExists(sourceTableName)) {
            System.err.println("源表不存在，无法导出数据: " + sourceTableName);
            return result;
        }

        try (Table table = connection.getTable(TableName.valueOf(sourceTableName))) {
            // 计算该日期的时间范围（从00:00:00到23:59:59）
            long startTimestamp = date.atStartOfDay(ZoneId.of("Asia/Shanghai")).toInstant().toEpochMilli();
            long endTimestamp = date.plusDays(1).atStartOfDay(ZoneId.of("Asia/Shanghai")).toInstant().toEpochMilli();

            // 扫描该日期范围内的所有数据
            Scan scan = new Scan();
            scan.setCaching(500); // 减小缓存
            scan.setMaxResultSize(10 * 1024 * 1024); // 10MB

            // 设置时间范围
            scan.setStartRow(Bytes.toBytes(startTimestamp));
            scan.setStopRow(Bytes.toBytes(endTimestamp));

            int count = 0;
            try (ResultScanner scanner = table.getScanner(scan)) {
                for (Result resultRow : scanner) {
                    count++;
                    if (count % 1000 == 0) {
                        System.out.println("已扫描 " + count + " 条记录");
                    }

                    byte[] rowKey = resultRow.getRow();

                    // 解析rowkey：时间戳(8字节) + 桩号(4字节)
                    if (rowKey.length >= 12) {
                        long timestamp = Bytes.toLong(rowKey, 0, 8);
                        int stakeNum = Bytes.toInt(rowKey, 8, 4);

                        byte[] valueBytes = resultRow.getValue(Bytes.toBytes("cf"), Bytes.toBytes("VehicleSegments"));
                        if (valueBytes != null) {
                            String jsonData = Bytes.toString(valueBytes);

                            VehicleMinuteData data = new VehicleMinuteData();
                            data.originalTimestamp = timestamp;
                            data.stakeNum = stakeNum;
                            data.jsonData = jsonData;

                            result.add(data);
                        }
                    }
                }
            }
            System.out.println("总计扫描 " + count + " 条记录");
        } catch (IOException e) {
            System.err.println("访问表 " + sourceTableName + " 时出错: " + e.getMessage());
            throw e;
        }

        return result;
    }

    // 复制数据到目标日期
    private static void replicateDataToDates(List<VehicleMinuteData> templateData, List<LocalDate> targetDates) throws IOException {
        Map<String, Table> tableCache = new HashMap<>();
        Map<String, List<Put>> putsByTable = new HashMap<>();
        int totalCount = 0;
        int skipCount = 0;
        long startTime = System.currentTimeMillis();

        try {
            System.out.println("开始复制数据，模板数据量: " + templateData.size());

            for (int i = 0; i < targetDates.size(); i++) {
                LocalDate targetDate = targetDates.get(i);

                // 每处理5天输出一次进度
                if (i % 5 == 0) {
                    System.out.println("处理进度: " + (i+1) + "/" + targetDates.size() + "，日期: " + targetDate);
                }

                // 检查当天是否已有数据
                if (checkDateHasData(targetDate)) {
                    System.out.println("日期 " + targetDate + " 的数据已存在，跳过");
                    skipCount++;
                    continue;
                }

                // 目标日期的时间偏移量
                long targetDayStart = targetDate.atStartOfDay(ZoneId.of("Asia/Shanghai")).toInstant().toEpochMilli();

                // 为每一天生成每分钟的数据
                for (VehicleMinuteData template : templateData) {
                    // 获取原时间戳对应的分钟信息
                    LocalDateTime originalDateTime = LocalDateTime.ofInstant(
                            Instant.ofEpochMilli(template.originalTimestamp),
                            ZoneId.of("Asia/Shanghai")
                    );

                    // 获取原时间的分钟（保留到分钟级别）
                    int hour = originalDateTime.getHour();
                    int minute = originalDateTime.getMinute();

                    // 创建新的时间戳（目标日期的相同时分）
                    LocalDateTime newDateTime = targetDate.atTime(hour, minute, 0, 0);
                    long newTimestamp = newDateTime.atZone(ZoneId.of("Asia/Shanghai")).toInstant().toEpochMilli();

                    // 更新JSON数据中的时间戳
                    String updatedJson = updateJsonTimestamp(template.jsonData, newTimestamp);

                    // 获取目标表名
                    String targetTableName = "JTSTCar_" + getQuarterTableName(newTimestamp);

                    // 确保目标表存在
                    try {
                        createTableIfNotExists(targetTableName);
                    } catch (IOException e) {
                        System.err.println("创建表失败: " + targetTableName + " - " + e.getMessage());
                        continue;
                    }

                    // 创建新的rowkey：时间戳(8字节) + 桩号(4字节)
                    byte[] newRowKey = Bytes.add(Bytes.toBytes(newTimestamp), Bytes.toBytes(template.stakeNum));

                    // 准备Put对象
                    Put put = new Put(newRowKey);
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("VehicleSegments"), Bytes.toBytes(updatedJson));

                    // 按表分组存放Put
                    putsByTable.computeIfAbsent(targetTableName, k -> new ArrayList<>()).add(put);
                    totalCount++;

                    // 批量提交
                    if (putsByTable.get(targetTableName).size() >= BATCH_SIZE) {
                        batchSaveToHBase(targetTableName, putsByTable.get(targetTableName), tableCache);
                        putsByTable.get(targetTableName).clear();
                    }

                    // 每处理1000条输出一次进度
                    if (totalCount % 1000 == 0) {
                        System.out.println("已处理 " + totalCount + " 条记录");
                    }
                }

                // 每处理一天强制提交一次，避免内存占用过高
                if (!putsByTable.isEmpty()) {
                    for (Map.Entry<String, List<Put>> entry : putsByTable.entrySet()) {
                        if (!entry.getValue().isEmpty()) {
                            batchSaveToHBase(entry.getKey(), entry.getValue(), tableCache);
                            entry.getValue().clear();
                        }
                    }
                }
            }

            // 提交剩余的数据
            for (Map.Entry<String, List<Put>> entry : putsByTable.entrySet()) {
                if (!entry.getValue().isEmpty()) {
                    batchSaveToHBase(entry.getKey(), entry.getValue(), tableCache);
                }
            }

        } finally {
            // 关闭所有表连接
            for (Table table : tableCache.values()) {
                try {
                    table.close();
                } catch (IOException e) {
                    System.err.println("关闭表连接时出错: " + e.getMessage());
                }
            }
        }

        long endTime = System.currentTimeMillis();
        System.out.println("数据复制完成，共写入 " + totalCount + " 条记录，跳过 " + skipCount + " 天，耗时: " + (endTime - startTime) / 1000 + " 秒");
    }

    // 检查指定日期是否已有数据
    private static boolean checkDateHasData(LocalDate date) {
        String quarter = getQuarterTableName(date);
        String tableName = "JTSTCar_" + quarter;

        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            // 计算该日期的时间范围
            long startTimestamp = date.atStartOfDay(ZoneId.of("Asia/Shanghai")).toInstant().toEpochMilli();
            long endTimestamp = date.plusDays(1).atStartOfDay(ZoneId.of("Asia/Shanghai")).toInstant().toEpochMilli();

            // 扫描一条数据检查是否存在
            Scan scan = new Scan();
            scan.setStartRow(Bytes.toBytes(startTimestamp));
            scan.setStopRow(Bytes.toBytes(endTimestamp));
            scan.setMaxResultSize(1);
            scan.setCaching(1);

            try (ResultScanner scanner = table.getScanner(scan)) {
                return scanner.iterator().hasNext();
            }
        } catch (IOException e) {
            // 表不存在或扫描出错，说明没有数据
            return false;
        }
    }

    // 更新JSON数据中的时间戳
    private static String updateJsonTimestamp(String jsonData, long newTimestamp) {
        try {
            JSONObject json = JSON.parseObject(jsonData);
            json.put("timeStamp", newTimestamp);
            return json.toJSONString();
        } catch (Exception e) {
            System.err.println("更新JSON时间戳失败: " + e.getMessage());
            return jsonData;
        }
    }

    // 批量保存到HBase
    private static void batchSaveToHBase(String tableName, List<Put> putList, Map<String, Table> tableCache) throws IOException {
        if (putList.isEmpty()) return;

        Table table = tableCache.computeIfAbsent(tableName, k -> {
            try {
                return connection.getTable(TableName.valueOf(tableName));
            } catch (IOException e) {
                throw new RuntimeException("获取表失败: " + tableName, e);
            }
        });

        int retryCount = 0;
        int maxRetries = 3;

        while (retryCount < maxRetries) {
            try {
                table.put(putList);
                System.out.println("批量写入表 " + tableName + ": " + putList.size() + " 条记录");
                return;
            } catch (IOException e) {
                retryCount++;
                System.err.println("批量写入失败 (尝试 " + retryCount + "/" + maxRetries + "): " + e.getMessage());

                if (retryCount >= maxRetries) {
                    throw e;
                }

                // 等待一段时间后重试
                try {
                    Thread.sleep(2000 * retryCount);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException("重试被中断", ie);
                }
            }
        }
    }

    // 数据容器类
    static class VehicleMinuteData {
        long originalTimestamp;  // 原始时间戳（毫秒）
        int stakeNum;            // 桩号
        String jsonData;         // VehicleSegments的JSON数据
    }

    // 清除指定日期之后的数据
    public static void clearDataFromDate(String startDate) throws IOException {
        System.out.println("开始清除从 " + startDate + " 开始的数据...");

        List<LocalDate> dates = generateDatesFromStartDate(2023, startDate);
        int deleteCount = 0;

        for (LocalDate date : dates) {
            String quarter = getQuarterTableName(date);
            String tableName = "JTSTCar_" + quarter;

            if (tableExists(tableName)) {
                deleteCount += deleteDayData(tableName, date);
            }
        }

        System.out.println("清除从 " + startDate + " 开始的数据完成，共删除 " + deleteCount + " 条记录");
    }

    // 删除指定日期的数据
    private static int deleteDayData(String tableName, LocalDate date) throws IOException {
        int count = 0;
        List<Delete> deleteList = new ArrayList<>();

        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            long startTimestamp = date.atStartOfDay(ZoneId.of("Asia/Shanghai")).toInstant().toEpochMilli();
            long endTimestamp = date.plusDays(1).atStartOfDay(ZoneId.of("Asia/Shanghai")).toInstant().toEpochMilli();

            Scan scan = new Scan();
            scan.setStartRow(Bytes.toBytes(startTimestamp));
            scan.setStopRow(Bytes.toBytes(endTimestamp));
            scan.setCaching(100);
            scan.setMaxResultSize(5 * 1024 * 1024);

            try (ResultScanner scanner = table.getScanner(scan)) {
                for (Result result : scanner) {
                    String rowKey = Bytes.toString(result.getRow());
                    Delete delete = new Delete(Bytes.toBytes(rowKey));
                    deleteList.add(delete);
                    count++;

                    if (deleteList.size() >= 200) {
                        table.delete(deleteList);
                        deleteList.clear();
                        System.out.println("删除批次: " + count + " 条（日期: " + date + "）");
                    }
                }

                // 提交剩余的删除
                if (!deleteList.isEmpty()) {
                    table.delete(deleteList);
                    deleteList.clear();
                }
            }
        } catch (IOException e) {
            System.err.println("删除日期 " + date + " 数据时出错: " + e.getMessage());
        }

        return count;
    }

    public static void main(String[] args) {
        System.out.println("=== JTSTCar数据复制工具 V2 ===");
        System.out.println("批量写入大小: " + BATCH_SIZE);

        try {
//            clearDataFromDate("20230101");
            // 指定从2023年8月19日开始继续复制
            String startDate = "20230101";

            System.out.println("开始从 " + startDate + " 继续复制数据...");

            // 检查可用的表
            System.out.println("检查可用的表...");
            listAllTables();

            // 跳过清除步骤，直接复制（会跳过已存在的数据）
            System.out.println("跳过清除步骤，直接复制（将跳过已存在的数据）");

            // 复制昨天数据到2023年指定日期范围
            System.out.println("开始复制昨天数据到2023年从 " + startDate + " 到年底...");
            replicateYesterdayTo2023(startDate);

        } catch (Exception e) {
            System.err.println("程序执行出错: " + e.getMessage());
            e.printStackTrace();
        } finally {
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (IOException e) {
                System.err.println("关闭连接时出错: " + e.getMessage());
            }
        }

        System.out.println("=== JTSTCar数据复制工具运行结束 ===");
    }
}