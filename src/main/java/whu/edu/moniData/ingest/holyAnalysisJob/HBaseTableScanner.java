package whu.edu.moniData.ingest.holyAnalysisJob;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.TemporalAdjusters;
import java.util.*;

public class HBaseTableScanner {

    public static void main(String[] args) {
        // 示例时间范围（2023年9月17日 00:00:00 到 2023年12月18日 00:00:00）
        long startTimestamp = 1694908800000L; // 2023-09-17 00:00:00
        long endTimestamp = 1702857600000L;   // 2023-12-18 00:00:00

        Map<String, KeyRange> scanPlan = generateScanPlan(startTimestamp, endTimestamp);

        // 打印扫描计划
        System.out.println("HBase 扫描计划:");
        for (Map.Entry<String, KeyRange> entry : scanPlan.entrySet()) {
            System.out.printf("\n表名: %s\n  - StartKey: %s\n  - EndKey: %s\n",
                    entry.getKey(),
                    entry.getValue().getStartKey(),
                    entry.getValue().getEndKey());
        }
    }

    /**
     * 生成HBase扫描计划
     *
     * @param startTimestamp 开始时间戳（毫秒）
     * @param endTimestamp   结束时间戳（毫秒）
     * @return 扫描计划，包含表名和对应的key范围
     */
    public static Map<String, KeyRange> generateScanPlan(long startTimestamp, long endTimestamp) {
        // 获取涉及的季度表
        Set<String> quarterTables = getQuarterTablesInRange(startTimestamp, endTimestamp);

        // 创建扫描计划
        Map<String, KeyRange> scanPlan = new TreeMap<>();

        // 为每个季度表生成key范围
        for (String table : quarterTables) {
            // 获取该季度表的时间范围
            long[] quarterRange = getQuarterTimeRange(table);
            long quarterStart = quarterRange[0];
            long quarterEnd = quarterRange[1];

            // 计算该表内实际需要扫描的时间范围
            long tableStart = Math.max(startTimestamp, quarterStart);
            long tableEnd = Math.min(endTimestamp, quarterEnd);

            // 生成该表的key范围
            KeyRange keyRange = generateKeyRangeForTable(tableStart, tableEnd);
            scanPlan.put(table, keyRange);
        }

        return scanPlan;
    }

    /**
     * 获取时间范围内的季度表
     */
    private static Set<String> getQuarterTablesInRange(long startTimestamp, long endTimestamp) {
        Set<String> tables = new TreeSet<>();

        LocalDateTime startDate = LocalDateTime.ofInstant(
                Instant.ofEpochMilli(startTimestamp), ZoneId.systemDefault());
        LocalDateTime endDate = LocalDateTime.ofInstant(
                Instant.ofEpochMilli(endTimestamp), ZoneId.systemDefault());

        // 计算季度范围
        int startYear = startDate.getYear();
        int startQuarter = (startDate.getMonthValue() - 1) / 3 + 1;
        int endYear = endDate.getYear();
        int endQuarter = (endDate.getMonthValue() - 1) / 3 + 1;

        // 添加所有涉及的季度表
        int currentYear = startYear;
        int currentQuarter = startQuarter;

        while (currentYear < endYear || (currentYear == endYear && currentQuarter <= endQuarter)) {
            tables.add(getQuarterTableName(currentYear, currentQuarter));

            // 移动到下一季度
            currentQuarter++;
            if (currentQuarter > 4) {
                currentQuarter = 1;
                currentYear++;
            }
        }

        return tables;
    }

    /**
     * 获取季度表的时间范围
     */
    private static long[] getQuarterTimeRange(String tableName) {
        // 解析表名获取年份和季度
        String[] parts = tableName.split("_");
        int year = Integer.parseInt(parts[1]);
        int quarter = Integer.parseInt(parts[2].substring(1));

        // 计算季度开始和结束时间
        LocalDate quarterStartDate = LocalDate.of(year, (quarter - 1) * 3 + 1, 1);
        LocalDate quarterEndDate = quarterStartDate.plusMonths(3)
                .with(TemporalAdjusters.firstDayOfMonth());

        // 转换为LocalDateTime
        LocalDateTime quarterStart = quarterStartDate.atStartOfDay();
        LocalDateTime quarterEnd = quarterEndDate.atStartOfDay();

        return new long[] {
                quarterStart.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                quarterEnd.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()
        };
    }

    /**
     * 为表生成key范围
     */
    private static KeyRange generateKeyRangeForTable(long startTimestamp, long endTimestamp) {
        // 按分钟取整
        long startKey = (startTimestamp / 60000) * 60000;
        long endKey = ((endTimestamp - 1) / 60000 + 1) * 60000;

        return new KeyRange(formatKey(startKey), formatKey(endKey));
    }

    /**
     * 格式化key
     */
    private static String formatKey(long timestamp) {
        return String.format("%013d", timestamp);
    }

    /**
     * 生成季度表名
     */
    private static String getQuarterTableName(int year, int quarter) {
        return "JTSTCar_" + year + "_Q" + quarter;
    }

    /**
     * Key范围内部类
     */
    public static class KeyRange {
        private final String startKey;
        private final String endKey;

        public KeyRange(String startKey, String endKey) {
            this.startKey = startKey;
            this.endKey = endKey;
        }

        public String getStartKey() {
            return startKey;
        }

        public String getEndKey() {
            return endKey;
        }
    }
}