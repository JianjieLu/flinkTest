package whu.edu.moniData.Utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
//java -cp /home/ljj/export/flinkTest-1.0-SNAPSHOT.jar whu.edu.moniData.Utils.HBaseJavaExporter ZJTCarTraj_20250713 /home/ljj/export/a.csv cf0:trajectory cf0:type
public class HBaseJavaExporter {
    public static void main(String[] args) throws IOException {
        // 检查参数数量
        if (args.length < 3) {
            System.err.println("Usage: java HBaseJavaExporter <tableName> <outputFileName> <family:column1> [<family:column2> ...]");
            System.err.println("Example 1 (single column): java HBaseJavaExporter ZJTCarTraj_20250714 /export/data.csv cf0:trajectory");
            System.err.println("Example 2 (multiple columns): java HBaseJavaExporter ZJTCarTraj_20250714 /export/data.csv cf0:trajectory cf0:timestamp cf1:speed");
            System.exit(1);
        }

        // 解析命令行参数
        String tableName = args[0];
        String outputFileName = args[1];
        List<String> columns = new ArrayList<>();
        for (int i = 2; i < args.length; i++) {
            columns.add(args[i]);
        }

        System.out.println("Starting export with parameters:");
        System.out.println("Table: " + tableName);
        System.out.println("Output: " + outputFileName);
        System.out.println("Columns to export:");
        for (String col : columns) {
            System.out.println("  " + col);
        }

        // 1. 配置 HBase
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142,10.48.53.80");

        // 2. 创建连接
        try (Connection connection = ConnectionFactory.createConnection(conf);
             Table table = connection.getTable(TableName.valueOf(tableName));
             BufferedWriter writer = new BufferedWriter(new FileWriter(outputFileName))) {

            // 3. 设置扫描器
            Scan scan = new Scan();
            scan.setCaching(500); // 提高扫描性能

            // 添加要导出的列
            for (String col : columns) {
                String[] parts = col.split(":");
                if (parts.length != 2) {
                    System.err.println("Invalid column format: " + col + ". Should be family:column");
                    continue;
                }
                byte[] family = Bytes.toBytes(parts[0]);
                byte[] qualifier = Bytes.toBytes(parts[1]);
                scan.addColumn(family, qualifier);
            }

            // 4. 扫描表并导出
            try (ResultScanner scanner = table.getScanner(scan)) {
                int count = 0;
                long startTime = System.currentTimeMillis();

                // 写入CSV表头
                writer.write("RowKey");
                for (String col : columns) {
                    writer.write("," + col.replace(':', '_')); // 替换冒号为下划线
                }
                writer.newLine();

                for (Result result : scanner) {
                    count++;
                    String rowkey = Bytes.toString(result.getRow());
                    writer.write(rowkey);

                    // 获取并写入所有列值
                    for (String col : columns) {
                        String[] parts = col.split(":");
                        if (parts.length != 2) continue;

                        byte[] family = Bytes.toBytes(parts[0]);
                        byte[] qualifier = Bytes.toBytes(parts[1]);

                        byte[] valueBytes = result.getValue(family, qualifier);
                        String value = (valueBytes != null) ?
                                Bytes.toString(valueBytes) : "NULL";

                        // 处理可能包含逗号的值（用引号括起来）
                        if (value.contains(",")) {
                            value = "\"" + value + "\"";
                        }

                        writer.write("," + value);
                    }
                    writer.newLine();

                    // 每1000行输出一次进度
                    if (count % 1000 == 0) {
                        long elapsed = System.currentTimeMillis() - startTime;
                        System.out.printf("Exported %,d rows in %.2f seconds%n",
                                count, elapsed / 1000.0);
                    }
                }
                System.out.println("====================================");
                System.out.printf("Total exported: %,d rows%n", count);
            }
            System.out.println("Export completed! File: " + outputFileName);
        } catch (Exception e) {
            System.err.println("Export failed: " + e.getMessage());
            e.printStackTrace();
            System.exit(2);
        }
    }
}
