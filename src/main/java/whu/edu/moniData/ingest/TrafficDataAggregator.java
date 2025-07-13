package whu.edu.moniData.ingest;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class TrafficDataAggregator {


    public static Map<Long, Integer> aggregateDaily(
            Map<Long, Integer> hourlyData,
            long startTime,
            long endTime
    ) {
        // 1. 确定时间范围边界（向下取整到天）
        long startDay = truncateToDay(startTime);
        long endDay = truncateToDay(endTime) + 86400000; // 结束天+1天

        // 2. 创建按天分组的结果集
        Map<Long, Integer> dailyData = new TreeMap<>();

        // 3. 初始化所有天段（包括可能没有数据的天）
        for (long day = startDay; day < endDay; day += 86400000) {
            dailyData.put(day, 0);
        }

        // 4. 聚合小时数据到天段
        for (Map.Entry<Long, Integer> entry : hourlyData.entrySet()) {
            long hourTimestamp = entry.getKey();
            int count = entry.getValue();

            // 计算当前小时所属的天
            long day = truncateToDay(hourTimestamp);

            // 累加到对应天段
            dailyData.put(day, dailyData.getOrDefault(day, 0) + count);
        }

        return dailyData;
    }

    /**
     * 将时间戳向下取整到天
     */
    private static long truncateToDay(long timestamp) {
        // 使用Java 8时间API确保正确处理时区
        java.time.Instant instant = java.time.Instant.ofEpochMilli(timestamp);
        java.time.ZonedDateTime zdt = instant.atZone(java.time.ZoneId.systemDefault());
        java.time.LocalDate localDate = zdt.toLocalDate();
        java.time.ZonedDateTime startOfDay = localDate.atStartOfDay(java.time.ZoneId.systemDefault());
        return startOfDay.toInstant().toEpochMilli();
    }


    public static Map<Long, Integer> aggregateHourly(Map<Long, Integer> minuteData, long startTime, long endTime) {
        // 1. 确定时间范围边界（向下取整到小时）
        long startHour = truncateToHour(startTime);
        long endHour = truncateToHour(endTime) + 3600000; // 结束小时+1小时

        // 2. 创建按小时分组的结果集
        Map<Long, Integer> hourlyData = new TreeMap<>();

        // 3. 初始化所有小时段（包括可能没有数据的小时）
        for (long hour = startHour; hour < endHour; hour += 3600000) {
            hourlyData.put(hour, 0);
        }

        // 4. 聚合分钟数据到小时段
        for (Map.Entry<Long, Integer> entry : minuteData.entrySet()) {
            long minuteTimestamp = entry.getKey();
            int count = entry.getValue();

            // 计算当前分钟所属的小时
            long hour = truncateToHour(minuteTimestamp);

            // 累加到对应小时段
            hourlyData.put(hour, hourlyData.getOrDefault(hour, 0) + count);
        }

        return hourlyData;
    }

    /**
     * 将时间戳向下取整到小时
     *
     * @param timestamp 原始时间戳（毫秒）
     * @return 整点小时的时间戳（毫秒）
     */
    private static long truncateToHour(long timestamp) {
        // 转换为带时区的时间对象
        ZonedDateTime zdt = Instant.ofEpochMilli(timestamp)
                .atZone(ZoneId.systemDefault());

        // 取整到小时
        ZonedDateTime hourStart = zdt.truncatedTo(java.time.temporal.ChronoUnit.HOURS);

        return hourStart.toInstant().toEpochMilli();
    }

    /**
     * 生成测试数据（实际应用中从HBase查询）
     */
    private static Map<Long, Integer> generateTestData(long startTime, long endTime) {
        Map<Long, Integer> data = new HashMap<>();

        // 每分钟生成一个随机流量值
        for (long minute = startTime; minute < endTime; minute += 60000) {
            // 随机流量值 (50-200)
            int count = 50 + (int)(Math.random() * 150);
            data.put(minute, count);
        }

        return data;
    }

    public static void main(String[] args) {
        Map<Integer, Integer> map = new HashMap<>();

        // 添加指定的键值对
        map.put(0, 150);
        map.put(1, 93);
        map.put(2, 11);
        map.put(3, 9);
        map.put(4, 11);
        map.put(5, 12);
        map.put(6, 10);
        map.put(7, 15);
        map.put(8, 14);
        map.put(9, 13);
        map.put(10, 12);
        map.put(11, 11);
        map.put(12, 13);
        map.put(13, 16);
        map.put(14, 18);
        map.put(15, 14);
        map.put(16, 16);
        map.put(17, 20);
        map.put(18, 9);
        Map<Long,Integer> mlUp=new HashMap<>();
        Map<Long,Integer> mlDown=new HashMap<>();
long thinTime=1751957332137L/600000*600000;
        long isnd=thinTime;
        for (Map.Entry<Integer, Integer> entry : map.entrySet()) {
            mlUp.put(isnd,entry.getValue());
            isnd+=60000;
        }

        mlUp = aggregateHourly(mlUp,1751957332137L,1751958332137L);
        System.out.println(mlUp);
        int ij=0;
        for (Map.Entry<Long, Integer> entry : mlUp.entrySet()) {
            System.out.println("  ");
            System.out.println(ij);
            System.out.println(entry.getValue());
            System.out.println(map);
            System.out.println("  ");
            map.put(ij,entry.getValue());

            ij++;
        }
        System.out.println(map);

        // 测试数据：2023-06-15 16:29:00 到 2023-06-15 17:02:00
        long startTime = ZonedDateTime.of(2023, 6, 15, 16, 29, 0, 0, ZoneId.systemDefault())
                .toInstant().toEpochMilli();
        long endTime = ZonedDateTime.of(2023, 6, 15, 17, 2, 0, 0, ZoneId.systemDefault())
                .toInstant().toEpochMilli();

        System.out.println(truncateToHour(1751958332137L));
        System.out.println(1751958332137L/3600000*3600000);
        // 生成分钟级测试数据
        Map<Long, Integer> minuteData = generateTestData(startTime, endTime);

        // 按小时聚合
        Map<Long, Integer> hourlyData = aggregateHourly(minuteData, startTime, endTime);

        // 打印结果
        System.out.println("分钟级数据 (" + minuteData.size() + " 分钟):");
        minuteData.forEach((minute, count) ->
                System.out.printf("  %s: %d 辆车%n",
                        formatTime(minute), count));

        System.out.println("\n小时级聚合数据:");
        hourlyData.forEach((hour, count) ->
                System.out.printf("  %s: %d 辆车%n",
                        formatTime(hour), count));
    }

    /**
     * 格式化时间戳为可读字符串
     */
    private static String formatTime(long timestamp) {
        return Instant.ofEpochMilli(timestamp)
                .atZone(ZoneId.systemDefault())
                .format(java.time.format.DateTimeFormatter.ofPattern("HH:mm"));
    }

}