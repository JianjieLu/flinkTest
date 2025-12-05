package whu.edu.moniDataXinghu;

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

    static {
        try {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142,10.48.53.80");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 主方法：复制昨天数据到2023年每一天
    public static void replicateYesterdayTo2023() throws IOException {
        // 1. 获取昨天的日期
        String yesterday = getYesterdayDate();
        System.out.println("获取昨天数据，日期: " + yesterday);

        // 2. 导出昨天所有类型的数据
        Map<String, List<IndicatorData>> yesterdayData = exportDateData(yesterday);

        // 3. 生成2023年所有日期
        List<String> dates2023 = generateAllDatesInYear(2023);

        // 4. 为2023年每一天复制数据
        replicateDataToDates(yesterdayData, dates2023);

        System.out.println("数据复制完成！将 " + yesterday + " 的数据复制到2023年 " + dates2023.size() + " 天");
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

        // 导出小时数据
        result.put("hour", exportTimeTypeData(table, "hour_" + date));
        // 导出天数据
        result.put("day", exportTimeTypeData(table, "day_" + date));
        // 导出月数据
        String month = date.substring(0, 6);
        result.put("month", exportTimeTypeData(table, "month_" + month));

        table.close();
        return result;
    }

    // 导出特定时间类型的数据
    private static List<IndicatorData> exportTimeTypeData(Table table, String prefix) throws IOException {
        List<IndicatorData> dataList = new ArrayList<>();
        Scan scan = new Scan();
        scan.setFilter(new PrefixFilter(Bytes.toBytes(prefix)));

        try (ResultScanner scanner = table.getScanner(scan)) {
            for (Result result : scanner) {
                String rowKey = Bytes.toString(result.getRow());
                String jsonData = Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("IndicatorsOfTime")));

                if (jsonData != null) {
                    try {
                        JSONObject indicators = JSON.parseObject(jsonData);

                        // 解析rowkey获取各个字段
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

        System.out.println("导出 " + prefix + " 数据: " + dataList.size() + " 条记录");
        return dataList;
    }

    // 生成2023年所有日期
    private static List<String> generateAllDatesInYear(int year) {
        List<String> dates = new ArrayList<>();
        LocalDate startDate = LocalDate.of(year, 1, 1);
        LocalDate endDate = LocalDate.of(year, 12, 31);

        LocalDate current = startDate;
        while (!current.isAfter(endDate)) {
            dates.add(current.format(DateTimeFormatter.ofPattern("yyyyMMdd")));
            current = current.plusDays(1);
        }

        System.out.println("生成 " + year + " 年日期: " + dates.size() + " 天");
        return dates;
    }

    // 复制数据到目标日期
    private static void replicateDataToDates(Map<String, List<IndicatorData>> templateData,
                                             List<String> targetDates) throws IOException {
        Table table = connection.getTable(TableName.valueOf("real_traffic_indicators"));
        int totalCount = 0;

        for (String targetDate : targetDates) {
            System.out.println("正在处理日期: " + targetDate);

            // 复制小时数据
            for (IndicatorData template : templateData.get("hour")) {
                String newRowKey = createNewRowKey("hour", targetDate, template);
                String newJsonData = createNewJsonData(template, "hour", targetDate);
                saveToHBase(table, newRowKey, newJsonData);
                totalCount++;
            }

            // 复制天数据
            for (IndicatorData template : templateData.get("day")) {
                String newRowKey = createNewRowKey("day", targetDate, template);
                String newJsonData = createNewJsonData(template, "day", targetDate);
                saveToHBase(table, newRowKey, newJsonData);
                totalCount++;
            }

            // 复制月数据（每月第一天生成月数据）
            if (targetDate.endsWith("01")) {
                String month = targetDate.substring(0, 6);
                for (IndicatorData template : templateData.get("month")) {
                    String newRowKey = createNewRowKey("month", month, template);
                    String newJsonData = createNewJsonData(template, "month", month);
                    saveToHBase(table, newRowKey, newJsonData);
                    totalCount++;
                }
            }

            if (totalCount % 1000 == 0) {
                System.out.println("已处理 " + totalCount + " 条记录");
            }
        }

        table.close();
        System.out.println("数据复制完成，共写入 " + totalCount + " 条记录");
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

    // 保存到HBase
    private static void saveToHBase(Table table, String rowKey, String jsonData) throws IOException {
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(
                Bytes.toBytes("cf"),
                Bytes.toBytes("IndicatorsOfTime"),
                Bytes.toBytes(jsonData)
        );
        table.put(put);
    }

    // 数据容器类
    static class IndicatorData {
        String timeType;
        String timeKey;
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

    // 可选：清除2023年现有数据（如果需要）
    public static void clear2023Data() throws IOException {
        Table table = connection.getTable(TableName.valueOf("real_traffic_indicators"));
        List<String> dates2023 = generateAllDatesInYear(2023);

        int deleteCount = 0;
        for (String date : dates2023) {
            // 删除小时数据
            deleteCount += deleteTimeTypeData(table, "hour_" + date);
            // 删除天数据
            deleteCount += deleteTimeTypeData(table, "day_" + date);
        }

        // 删除月数据
        for (int month = 1; month <= 12; month++) {
            String monthStr = String.format("2023%02d", month);
            deleteCount += deleteTimeTypeData(table, "month_" + monthStr);
        }

        table.close();
        System.out.println("清除2023年数据完成，共删除 " + deleteCount + " 条记录");
    }

    private static int deleteTimeTypeData(Table table, String prefix) throws IOException {
        int count = 0;
        Scan scan = new Scan();
        scan.setFilter(new PrefixFilter(Bytes.toBytes(prefix)));

        try (ResultScanner scanner = table.getScanner(scan)) {
            List<Delete> deletes = new ArrayList<>();
            for (Result result : scanner) {
                String rowKey = Bytes.toString(result.getRow());
                Delete delete = new Delete(Bytes.toBytes(rowKey));
                deletes.add(delete);
                count++;

                if (deletes.size() >= 1000) {
                    table.delete(deletes);
                    deletes.clear();
                }
            }

            if (!deletes.isEmpty()) {
                table.delete(deletes);
            }
        }

        return count;
    }

    public static void main(String[] args) throws IOException {
        // 使用示例

        // 可选：先清除2023年现有数据
        // clear2023Data();

        // 复制昨天数据到2023年
        replicateYesterdayTo2023();
    }
}