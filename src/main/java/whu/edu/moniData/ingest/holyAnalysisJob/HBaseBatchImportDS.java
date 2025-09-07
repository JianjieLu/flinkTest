package whu.edu.moniData.ingest.holyAnalysisJob;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseBatchImportDS {

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: HBaseBatchImport <tableName> <inputFile> [batchSize]");
            System.out.println("Example: HBaseBatchImport ZCarTraj /data/trajectory.txt 1000");
            System.exit(1);
        }

        String tableName = args[0];
        String inputFile = args[1];
        int batchSize = args.length > 2 ? Integer.parseInt(args[2]) : 1000;

        try {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142,10.48.53.80");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            conf.set("zookeeper.session.timeout", "120000");
            conf.set("fs.defaultFS", "hdfs://100.65.38.139:9000");
            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

            Connection connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(TableName.valueOf(tableName));

            // 确保表存在
            createTableIfNotExists(connection, tableName, "cf0");

            // 批量导入数据
            batchImportData(table, inputFile, batchSize);

            table.close();
            connection.close();
            System.out.println("批量导入完成!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void createTableIfNotExists(Connection connection, String tableName, String columnFamily)
            throws IOException {
        try (Admin admin = connection.getAdmin()) {
            TableName hbaseTableName = TableName.valueOf(tableName);
            if (!admin.tableExists(hbaseTableName)) {
                HTableDescriptor tableDescriptor = new HTableDescriptor(hbaseTableName);
                tableDescriptor.addFamily(new HColumnDescriptor(columnFamily));
                admin.createTable(tableDescriptor);
                System.out.println("表 " + tableName + " 创建成功");
            } else {
                System.out.println("表 " + tableName + " 已存在");
            }
        }
    }

    private static void batchImportData(Table table, String inputFile, int batchSize)
            throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(inputFile));
        String line;
        List<Put> puts = new ArrayList<>(batchSize);
        int count = 0;

        System.out.println("开始读取文件: " + inputFile);

        while ((line = reader.readLine()) != null) {
            // 解析数据行，这里假设每行是逗号分隔的数据
            // 格式: rowKey,type,latestTime,trajectory,direction,vehicleColor,vehicleWeight,specialFlag,eventList
            String[] parts = line.split(",");
            if (parts.length < 9) {
                System.err.println("数据格式错误，跳过: " + line);
                continue;
            }

            String rowKey = parts[0];
            Put put = new Put(Bytes.toBytes(rowKey));

            // 添加列数据
            put.addColumn(Bytes.toBytes("cf0"), Bytes.toBytes("type"), Bytes.toBytes(parts[1]));
            put.addColumn(Bytes.toBytes("cf0"), Bytes.toBytes("latest_time"), Bytes.toBytes(parts[2]));
            put.addColumn(Bytes.toBytes("cf0"), Bytes.toBytes("trajectory"), Bytes.toBytes(parts[3]));
            put.addColumn(Bytes.toBytes("cf0"), Bytes.toBytes("direction"), Bytes.toBytes(parts[4]));
            put.addColumn(Bytes.toBytes("cf0"), Bytes.toBytes("vehicle_color"), Bytes.toBytes(parts[5]));
            put.addColumn(Bytes.toBytes("cf0"), Bytes.toBytes("vehicle_weight"), Bytes.toBytes(parts[6]));
            put.addColumn(Bytes.toBytes("cf0"), Bytes.toBytes("special_flag"), Bytes.toBytes(parts[7]));
            put.addColumn(Bytes.toBytes("cf0"), Bytes.toBytes("event_list"), Bytes.toBytes(parts[8]));

            puts.add(put);
            count++;

            // 批量提交
            if (puts.size() >= batchSize) {
                table.put(puts);
                puts.clear();
                System.out.println("已导入 " + count + " 条数据");
            }
        }

        // 提交剩余的数据
        if (!puts.isEmpty()) {
            table.put(puts);
            System.out.println("导入最后 " + puts.size() + " 条数据");
        }

        reader.close();
        System.out.println("总共导入 " + count + " 条数据");
    }
}