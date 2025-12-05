package whu.edu.moniData.ingest.holyAnalysisJob.redisAndHbase.tool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import whu.edu.moniData.ingest.holyAnalysisJob.redisAndHbase.buqua1n3HbaseWithAvgSpeedJTData;

import java.io.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class HBaseDataReplicator {

    private static Connection connection;
    private static final int BATCH_SIZE = 500; // 减小批量提交大小，避免超时
    private static final int DELETE_BATCH_SIZE = 200; // 删除批量大小更小

    static {
        try {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142,10.48.53.80");
            conf.set("hbase.zookeeper.property.clientPort", "2181");

            // 优化连接配置，避免超时
            conf.set("hbase.client.write.buffer", "2097152"); // 2MB缓冲区
            conf.set("hbase.client.max.perregion.tasks", "50");
            conf.set("hbase.rpc.timeout", "300000"); // 5分钟
            conf.set("hbase.client.operation.timeout", "300000"); // 5分钟
            conf.set("hbase.client.scanner.timeout.period", "300000"); // 5分钟
            conf.set("hbase.client.pause", "1000"); // 重试间隔
            conf.set("hbase.client.retries.number", "10"); // 重试次数
            conf.set("hbase.ipc.client.socket.timeout.read", "300000"); // 读超时
            conf.set("hbase.ipc.client.socket.timeout.write", "300000"); // 写超时

            // 启用连接池
            conf.set("hbase.client.ipc.pool.size", "10");
            conf.set("hbase.client.ipc.pool.type", "RoundRobin");

            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 主方法：复制昨天数据到2023年指定日期范围
    public static void replicateYesterdayTo2023(String startDate) throws IOException {
        long startTime = System.currentTimeMillis();

        // 1. 获取昨天的日期
        String yesterday = getYesterdayDate();
        System.out.println("获取昨天数据，日期: " + yesterday);

        // 2. 导出昨天所有类型的数据（包含小时数据）
        Map<String, List<IndicatorData>> yesterdayData = exportDateData(yesterday);

        // 3. 生成2023年从指定日期到年底的日期
        List<String> dates2023 = generateDatesFromStartDate(2023, startDate);

        // 4. 为2023年指定日期范围复制数据（跳过已存在的日期）
        replicateDataToDates(yesterdayData, dates2023);

        long endTime = System.currentTimeMillis();
        System.out.println("数据复制完成！耗时: " + (endTime - startTime) / 1000 + " 秒");
    }

    // 获取昨天日期 (yyyyMMdd格式)
    private static String getYesterdayDate() {
        LocalDate yesterday = LocalDate.now().minusDays(1);
        return yesterday.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
    }

    // 导出指定日期的所有数据
    private static Map<String, List<IndicatorData>> exportDateData(String date) throws IOException {
        Map<String, List<IndicatorData>> result = new HashMap<>();
        Table table = connection.getTable(TableName.valueOf("real_traffic_indicators"));

        try {
            // 导出小时数据 - 使用日期前缀获取该天所有小时数据
            result.put("hour", exportHourData(table, date));

            // 导出天数据
            List<IndicatorData> dayData = exportTimeTypeData(table, "day_" + date);
            result.put("day", dayData);
            System.out.println("导出天数据: " + dayData.size() + " 条");

            // 导出月数据
            String month = date.substring(0, 6);
            List<IndicatorData> monthData = exportTimeTypeData(table, "month_" + month);
            result.put("month", monthData);
            System.out.println("导出月数据: " + monthData.size() + " 条");
        } finally {
            table.close();
        }

        return result;
    }

    // 导出小时数据 - 支持完整的小时格式
    private static List<IndicatorData> exportHourData(Table table, String date) throws IOException {
        List<IndicatorData> dataList = new ArrayList<>();

        // 使用日期前缀扫描该天所有小时数据
        Scan scan = new Scan();
        scan.setFilter(new PrefixFilter(Bytes.toBytes("hour_" + date)));
        scan.setCaching(500); // 减小缓存大小
        scan.setMaxResultSize(10 * 1024 * 1024); // 10MB最大结果大小

        try (ResultScanner scanner = table.getScanner(scan)) {
            int count = 0;
            for (Result result : scanner) {
                count++;
                if (count % 1000 == 0) {
                    System.out.println("已扫描小时数据: " + count + " 条");
                }

                String rowKey = Bytes.toString(result.getRow());
                String jsonData = Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("IndicatorsOfTime")));

                if (jsonData != null) {
                    try {
                        // 解析rowkey: hour_yyyyMMddHH_laneNo_direction_stake
                        String[] parts = rowKey.split("_");
                        if (parts.length >= 5) {
                            String timeType = parts[0];
                            String timeKey = parts[1]; // yyyyMMddHH格式

                            // 提取小时
                            String hour = "";
                            if (timeKey.length() == 10) { // yyyyMMddHH格式
                                hour = timeKey.substring(8); // 最后两位是小时
                            }

                            int laneNo = Integer.parseInt(parts[2]);
                            int direction = Integer.parseInt(parts[3]);
                            int stake = Integer.parseInt(parts[4]);

                            JSONObject indicators = JSON.parseObject(jsonData);

                            IndicatorData data = new IndicatorData();
                            data.timeType = timeType;
                            data.timeKey = timeKey; // 完整的时间键 yyyyMMddHH
                            data.date = timeKey.substring(0, 8); // 日期部分 yyyyMMdd
                            data.hour = hour; // 小时部分
                            data.laneNo = laneNo;
                            data.direction = direction;
                            data.stake = stake;
                            data.occupancy = indicators.getDoubleValue("occupancy");
                            data.headway = indicators.getDoubleValue("headway");
                            data.delayIndex = indicators.getDoubleValue("delay_index");
                            data.vehicleCount = indicators.getIntValue("vehicle_count");
                            data.busAvgSpeed = indicators.getDoubleValue("bus_avg_speed");
                            data.trackAvgSpeed = indicators.getDoubleValue("track_avg_speed");
                            data.busCount = indicators.getIntValue("bus_count");
                            data.trackCount = indicators.getIntValue("track_count");
                            data.originalRowKey = rowKey;
                            data.jsonData = jsonData;

                            dataList.add(data);
                        }
                    } catch (Exception e) {
                        System.err.println("解析小时数据失败: " + rowKey + " - " + e.getMessage());
                    }
                }
            }
        }

        System.out.println("导出小时数据: " + dataList.size() + " 条（包含小时维度）");
        return dataList;
    }

    // 导出天和月数据
    private static List<IndicatorData> exportTimeTypeData(Table table, String prefix) throws IOException {
        List<IndicatorData> dataList = new ArrayList<>();
        Scan scan = new Scan();
        scan.setFilter(new PrefixFilter(Bytes.toBytes(prefix)));
        scan.setCaching(500); // 减小缓存大小
        scan.setMaxResultSize(10 * 1024 * 1024); // 10MB最大结果大小

        try (ResultScanner scanner = table.getScanner(scan)) {
            int count = 0;
            for (Result result : scanner) {
                count++;
                if (count % 500 == 0) {
                    System.out.println("已扫描天/月数据: " + count + " 条");
                }

                String rowKey = Bytes.toString(result.getRow());
                String jsonData = Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("IndicatorsOfTime")));

                if (jsonData != null) {
                    try {
                        JSONObject indicators = JSON.parseObject(jsonData);

                        // 解析rowkey
                        String[] parts = rowKey.split("_");
                        if (parts.length >= 5) {
                            String timeType = parts[0];
                            String timeKey = parts[1];
                            int laneNo = Integer.parseInt(parts[2]);
                            int direction = Integer.parseInt(parts[3]);
                            int stake = Integer.parseInt(parts[4]);

                            IndicatorData data = new IndicatorData();
                            data.timeType = timeType;
                            data.timeKey = timeKey;
                            data.date = timeKey.length() >= 8 ? timeKey.substring(0, 8) : timeKey;
                            data.hour = "";
                            data.laneNo = laneNo;
                            data.direction = direction;
                            data.stake = stake;
                            data.occupancy = indicators.getDoubleValue("occupancy");
                            data.headway = indicators.getDoubleValue("headway");
                            data.delayIndex = indicators.getDoubleValue("delay_index");
                            data.vehicleCount = indicators.getIntValue("vehicle_count");
                            data.busAvgSpeed = indicators.getDoubleValue("bus_avg_speed");
                            data.trackAvgSpeed = indicators.getDoubleValue("track_avg_speed");
                            data.busCount = indicators.getIntValue("bus_count");
                            data.trackCount = indicators.getIntValue("track_count");
                            data.originalRowKey = rowKey;
                            data.jsonData = jsonData;

                            dataList.add(data);
                        }
                    } catch (Exception e) {
                        System.err.println("解析数据失败: " + rowKey + " - " + e.getMessage());
                    }
                }
            }
        }

        return dataList;
    }

    // 生成2023年从指定日期到年底的所有日期
    private static List<String> generateDatesFromStartDate(int year, String startDateStr) {
        List<String> dates = new ArrayList<>();

        // 解析开始日期
        int yearPart = Integer.parseInt(startDateStr.substring(0, 4));
        int monthPart = Integer.parseInt(startDateStr.substring(4, 6));
        int dayPart = Integer.parseInt(startDateStr.substring(6, 8));

        LocalDate startDate = LocalDate.of(yearPart, monthPart, dayPart);
        LocalDate endDate = LocalDate.of(year, 12, 31);

        LocalDate current = startDate;
        while (!current.isAfter(endDate)) {
            dates.add(current.format(DateTimeFormatter.ofPattern("yyyyMMdd")));
            current = current.plusDays(1);
        }

        System.out.println("生成从 " + startDateStr + " 到 2023年年底的日期: " + dates.size() + " 天");
        return dates;
    }

    // 检查指定前缀的数据是否存在（只要有一条就返回true）
    private static boolean checkDataExists(Table table, String prefix) throws IOException {
        Scan scan = new Scan();
        scan.setFilter(new PrefixFilter(Bytes.toBytes(prefix)));
        scan.setMaxResultSize(1);
        scan.setCaching(1);

        try (ResultScanner scanner = table.getScanner(scan)) {
            return scanner.iterator().hasNext();
        }
    }

    // 复制数据到目标日期（批量写入）
    private static void replicateDataToDates(Map<String, List<IndicatorData>> templateData,
                                             List<String> targetDates) throws IOException {
        Table table = connection.getTable(TableName.valueOf("real_traffic_indicators"));
        List<Put> putList = new ArrayList<>();
        int totalCount = 0;
        int skipCount = 0;
        long startTime = System.currentTimeMillis();

        try {
            List<IndicatorData> hourTemplates = templateData.get("hour");
            int hourTemplateCount = hourTemplates != null ? hourTemplates.size() : 0;

            System.out.println("开始复制数据，小时模板数: " + hourTemplateCount);
            System.out.println("天模板数: " + templateData.get("day").size());
            System.out.println("月模板数: " + templateData.get("month").size());

            for (int i = 0; i < targetDates.size(); i++) {
                String targetDate = targetDates.get(i);

                // 每处理10天输出一次进度
                if (i % 10 == 0) {
                    System.out.println("正在处理日期: " + targetDate + " (进度: " + (i+1) + "/" + targetDates.size() + ")");
                }

                // 检查当天数据是否已存在（检查小时数据即可，只要有一条就认为已存在）
                String hourPrefix = "hour_" + targetDate;
                try {
                    if (checkDataExists(table, hourPrefix)) {
                        System.out.println("日期 " + targetDate + " 的数据已存在，跳过");
                        skipCount++;
                        continue;
                    }
                } catch (Exception e) {
                    System.err.println("检查日期 " + targetDate + " 数据存在性时出错: " + e.getMessage());
                    // 继续尝试复制，即使检查失败
                }

                // 复制小时数据 - 为每一天的每个小时复制数据
                if (hourTemplates != null) {
                    for (IndicatorData template : hourTemplates) {
                        // 保留原始的小时信息，只替换日期
                        String targetDateTimeKey = targetDate + template.hour;

                        String newRowKey = createNewRowKey("hour", targetDateTimeKey, template);
                        String newJsonData = createNewJsonData(template, "hour", targetDateTimeKey);

                        Put put = new Put(Bytes.toBytes(newRowKey));
                        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("IndicatorsOfTime"), Bytes.toBytes(newJsonData));
                        putList.add(put);
                        totalCount++;

                        // 批量提交
                        if (putList.size() >= BATCH_SIZE) {
                            batchSaveToHBase(table, putList);
                            putList.clear();
                        }
                    }
                }

                // 复制天数据
                for (IndicatorData template : templateData.get("day")) {
                    String newRowKey = createNewRowKey("day", targetDate, template);
                    String newJsonData = createNewJsonData(template, "day", targetDate);

                    Put put = new Put(Bytes.toBytes(newRowKey));
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("IndicatorsOfTime"), Bytes.toBytes(newJsonData));
                    putList.add(put);
                    totalCount++;

                    // 批量提交
                    if (putList.size() >= BATCH_SIZE) {
                        batchSaveToHBase(table, putList);
                        putList.clear();
                    }
                }

                // 复制月数据（每月第一天生成月数据）
                if (targetDate.endsWith("01")) {
                    String month = targetDate.substring(0, 6);
                    String monthPrefix = "month_" + month;
                    try {
                        if (!checkDataExists(table, monthPrefix)) {
                            for (IndicatorData template : templateData.get("month")) {
                                String newRowKey = createNewRowKey("month", month, template);
                                String newJsonData = createNewJsonData(template, "month", month);

                                Put put = new Put(Bytes.toBytes(newRowKey));
                                put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("IndicatorsOfTime"), Bytes.toBytes(newJsonData));
                                putList.add(put);
                                totalCount++;

                                // 批量提交
                                if (putList.size() >= BATCH_SIZE) {
                                    batchSaveToHBase(table, putList);
                                    putList.clear();
                                }
                            }
                        }
                    } catch (Exception e) {
                        System.err.println("处理月数据时出错: " + e.getMessage());
                    }
                }

                // 每处理5天强制提交一次，避免内存占用过高
                if (i % 5 == 0 && !putList.isEmpty()) {
                    batchSaveToHBase(table, putList);
                    putList.clear();
                    System.out.println("强制提交，已写入 " + totalCount + " 条");
                }
            }

            // 提交剩余的数据
            if (!putList.isEmpty()) {
                batchSaveToHBase(table, putList);
                putList.clear();
            }
        } finally {
            table.close();
        }

        long endTime = System.currentTimeMillis();
        System.out.println("数据复制完成，共写入 " + totalCount + " 条记录，跳过 " + skipCount + " 天，耗时: " + (endTime - startTime) / 1000 + " 秒");
    }

    // 批量保存到HBase
    private static void batchSaveToHBase(Table table, List<Put> putList) throws IOException {
        if (putList.isEmpty()) return;

        int retryCount = 0;
        int maxRetries = 3;

        while (retryCount < maxRetries) {
            try {
                table.put(putList);
                System.out.println("批量写入 " + putList.size() + " 条记录成功");
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

    // 创建新的rowkey
    private static String createNewRowKey(String timeType, String timeKey, IndicatorData template) {
        return String.format("%s_%s_%d_%d_%d",
                timeType, timeKey, template.laneNo, template.direction, template.stake);
    }

    // 创建新的JSON数据
    private static String createNewJsonData(IndicatorData template, String timeType, String timeKey) {
        buqua1n3HbaseWithAvgSpeedJTData.IndicatorsOfTime indicators =
                new buqua1n3HbaseWithAvgSpeedJTData.IndicatorsOfTime(
                        timeType,
                        template.occupancy,
                        template.headway,
                        template.delayIndex,
                        template.vehicleCount,
                        template.busAvgSpeed,
                        template.trackAvgSpeed,
                        template.busCount,
                        template.trackCount
                );

        return JSON.toJSONString(indicators);
    }

    // 数据容器类
    static class IndicatorData {
        String timeType;
        String timeKey;      // 完整的时间键，如：2025112920（小时数据）、20251129（天数据）
        String date;         // 日期部分，如：20251129
        String hour;         // 小时部分，如：20（仅小时数据有）
        int laneNo;
        int direction;
        int stake;
        double occupancy;
        double headway;
        double delayIndex;
        int vehicleCount;
        double busAvgSpeed;
        double trackAvgSpeed;
        int busCount;
        int trackCount;
        String originalRowKey;
        String jsonData;
    }

    // 清除指定日期之后的数据 - 优化的版本
    public static void clearDataFromDate(String startDate) throws IOException {
        System.out.println("开始清除从 " + startDate + " 开始的数据...");

        // 先跳过清除步骤，直接复制，因为复制方法会检查数据是否存在
        System.out.println("跳过清除步骤，直接复制（将跳过已存在的数据）");

        // 如果确实需要清除，可以使用下面的方法，但建议分批次进行
        // clearDataFromDateBatch(startDate);
    }

    // 分批次清除数据
    private static void clearDataFromDateBatch(String startDate) throws IOException {
        Table table = connection.getTable(TableName.valueOf("real_traffic_indicators"));
        List<String> dates = generateDatesFromStartDate(2023, startDate);

        int deleteCount = 0;
        int batchCount = 0;

        try {
            for (int i = 0; i < dates.size(); i++) {
                String date = dates.get(i);

                if (i % 10 == 0) {
                    System.out.println("清除进度: " + (i+1) + "/" + dates.size() + "，已删除 " + deleteCount + " 条");
                }

                // 删除小时数据 - 分小时处理，避免一次扫描太多数据
                deleteCount += deleteHourDataByDay(table, date);

                // 删除天数据
                deleteCount += deleteTimeTypeData(table, "day_" + date);

                // 如果是每月的第一天，也删除该月的月数据
                if (date.endsWith("01")) {
                    String month = date.substring(0, 6);
                    deleteCount += deleteTimeTypeData(table, "month_" + month);
                }

                // 每处理5天休息一下，避免给HBase太大压力
                if (i % 5 == 0) {
                    System.out.println("暂停2秒...");
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        } finally {
            table.close();
        }

        System.out.println("清除从 " + startDate + " 开始的数据完成，共删除 " + deleteCount + " 条记录");
    }

    // 按天分小时删除数据
    private static int deleteHourDataByDay(Table table, String date) throws IOException {
        int totalDeleted = 0;

        // 分小时删除，避免一次扫描24小时的数据
        for (int hour = 0; hour < 24; hour++) {
            String hourStr = String.format("%02d", hour);
            String hourPrefix = "hour_" + date + hourStr;
            totalDeleted += deleteTimeTypeData(table, hourPrefix);
        }

        return totalDeleted;
    }

    // 删除指定前缀的数据
    private static int deleteTimeTypeData(Table table, String prefix) throws IOException {
        int count = 0;
        List<Delete> deleteList = new ArrayList<>();

        Scan scan = new Scan();
        scan.setFilter(new PrefixFilter(Bytes.toBytes(prefix)));
        scan.setCaching(100); // 更小的缓存
        scan.setMaxResultSize(5 * 1024 * 1024); // 5MB

        try (ResultScanner scanner = table.getScanner(scan)) {
            for (Result result : scanner) {
                String rowKey = Bytes.toString(result.getRow());
                Delete delete = new Delete(Bytes.toBytes(rowKey));
                deleteList.add(delete);
                count++;

                if (deleteList.size() >= DELETE_BATCH_SIZE) {
                    table.delete(deleteList);
                    deleteList.clear();
                    System.out.println("删除批次: " + count + " 条（前缀: " + prefix + "）");
                }
            }

            // 提交剩余的删除
            if (!deleteList.isEmpty()) {
                table.delete(deleteList);
                deleteList.clear();
            }
        } catch (Exception e) {
            System.err.println("删除前缀 " + prefix + " 时出错: " + e.getMessage());
            // 继续处理下一个前缀
        }

        return count;
    }

    public static void main(String[] args) throws IOException {
        System.out.println("=== HBase数据复制工具开始运行 ===");
        System.out.println("批量写入大小: " + BATCH_SIZE);
        clearDataFromDateBatch("20230101");
        // 指定从2023年8月19日开始继续复制
        String startDate = "20230101";

        System.out.println("开始从 " + startDate + " 继续复制数据...");

        // 跳过清除步骤，直接复制（会跳过已存在的数据）
        System.out.println("跳过清除步骤，直接复制（将跳过已存在的数据）");

        // 复制昨天数据到2023年指定日期范围
        System.out.println("开始复制昨天数据到2023年从 " + startDate + " 到年底...");
        try {
            replicateYesterdayTo2023(startDate);
        } catch (Exception e) {
            System.err.println("复制过程中出错: " + e.getMessage());
            e.printStackTrace();
        }

        System.out.println("=== HBase数据复制工具运行结束 ===");
    }
}