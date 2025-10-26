package whu.edu.moniData.ingest.holyAnalysisJob.redisAndHbase.agoVersions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
public class HBaseDataQuery {

    // HBase 配置
    private static final Configuration conf = HBaseConfiguration.create();
    static {
        conf.set("hbase.zookeeper.quorum", "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142,10.48.53.80");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
    }

    // 表名和列族
    private static final String TABLE_NAME = "tabl";
    private static final String COLUMN_FAMILY = "f1";

    // 时间格式
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter ROWKEY_FORMAT = DateTimeFormatter.ofPattern("yyyyMMddHH");

    public static void main(String[] args) {
        try {
            // 查询 2023-10-26 17:00 到 18:00 的数据
            LocalDateTime startTime = LocalDateTime.of(2025, 10, 26, 17, 0);
            LocalDateTime endTime = LocalDateTime.of(2025, 10, 26, 18, 0);

            List<Map<String, String>> results = queryDataByTimeRange(startTime, endTime);

            // 打印结果
            System.out.println("查询结果 (" + results.size() + " 条记录):");
            for (Map<String, String> record : results) {
                System.out.println("RowKey: " + record.get("rowKey"));
                System.out.println("下行公交: " + record.get("downBus"));
                System.out.println("下行轨道: " + record.get("downTrack"));
                System.out.println("上行公交: " + record.get("upBus"));
                System.out.println("上行轨道: " + record.get("upTrack"));
                System.out.println("上行总数: " + record.get("upCount"));
                System.out.println("下行总数: " + record.get("downCount"));
                System.out.println("--------------------------------");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 根据时间范围查询数据
     *
     * @param startTime 开始时间 (包含)
     * @param endTime   结束时间 (不包含)
     * @return 查询结果列表
     */
    public static List<Map<String, String>> queryDataByTimeRange(LocalDateTime startTime, LocalDateTime endTime)
            throws IOException {

        List<Map<String, String>> results = new ArrayList<>();

        try (Connection connection = ConnectionFactory.createConnection(conf);
             Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {

            // 1. 获取所有可能的 orgcode (从配置中获取)
            Set<String> orgcodes = getOrgcodesFromConfig();

            // 2. 为每个 orgcode 构建扫描范围
            for (String orgcode : orgcodes) {
                // 计算时间范围内的所有小时
                LocalDateTime currentHour = startTime.withMinute(0).withSecond(0).withNano(0);
                while (currentHour.isBefore(endTime)) {
                    // 构建 RowKey 前缀: orgcode + "_" + 时间戳(小时级)
                    String rowKeyPrefix = orgcode + "_" + currentHour.format(ROWKEY_FORMAT);

                    // 创建扫描对象
                    Scan scan = new Scan();
                    scan.setRowPrefixFilter(Bytes.toBytes(rowKeyPrefix));

                    // 执行扫描
                    try (ResultScanner scanner = table.getScanner(scan)) {
                        for (Result result : scanner) {
                            Map<String, String> record = parseResult(result);
                            results.add(record);
                        }
                    }

                    // 移动到下一小时
                    currentHour = currentHour.plusHours(1);
                }
            }
        }

        return results;
    }

    /**
     * 从配置获取所有 orgcode
     */
    private static Set<String> getOrgcodesFromConfig() {
        // 这里应该从实际配置源获取 orgcode 列表
        // 示例: 返回硬编码的 orgcode 列表
        return new HashSet<>(Arrays.asList(
                "C7370151-2116-470A-8E26-5F878B3C9D78"
                // 添加更多 orgcode...
        ));
    }

    /**
     * 解析 HBase 查询结果
     */
    private static Map<String, String> parseResult(Result result) {
        Map<String, String> record = new HashMap<>();

        // 获取 RowKey
        String rowKey = Bytes.toString(result.getRow());
        record.put("rowKey", rowKey);

        // 解析列值
        record.put("downBus", getColumnValue(result, "downBus"));
        record.put("downTrack", getColumnValue(result, "downTrack"));
        record.put("upBus", getColumnValue(result, "upBus"));
        record.put("upTrack", getColumnValue(result, "upTrack"));
        record.put("upCount", getColumnValue(result, "upCount"));
        record.put("downCount", getColumnValue(result, "downCount"));

        return record;
    }

    /**
     * 获取列值
     */
    private static String getColumnValue(Result result, String column) {
        byte[] value = result.getValue(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(column));
        return value != null ? Bytes.toString(value) : "N/A";
    }
}