
package whu.edu.moniData.Utils;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
        import org.apache.hadoop.hbase.client.*;
        import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import whu.edu.moniData.Utils.TrafficEventUtils.*;
        import javafx.util.Pair;

public class totalOps {

    public static List<Pair<whu.edu.moniData.Utils.TrafficEventUtils.CongestionEvent,Long>>getCongestionEvent(String tableName, List<Long> time){
        List<Pair<whu.edu.moniData.Utils.TrafficEventUtils.CongestionEvent,Long>> congs = new ArrayList<>();
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142,100.65.38.36,100.65.38.37,100.65.38.38");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        List<String> ss=new ArrayList<>();
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
            // 检查表是否存在
            if (!isTableExists(connection, tableName)) {
                System.out.println("表 " + tableName + " 不存在");
                return congs; // 直接返回空列表
            }else{
                System.out.println("查找表 " + tableName);
            }

            try (Table table = connection.getTable(TableName.valueOf(tableName))) {
                for(long t:time){
                    Get get = new Get(String.valueOf(t).getBytes());
                    Result result1 = table.get(get);
                    for (Cell cell : result1.rawCells()) {
                        System.out.println("Row Key: " + Bytes.toString(result1.getRow())+"Value: " + Bytes.toString(CellUtil.cloneValue(cell)));
                        JSONArray objects = JSON.parseArray(Bytes.toString(CellUtil.cloneValue(cell)));
                        for (Object object : objects) {
                            congs.add(new Pair<>(JSON.parseObject(object.toString(), whu.edu.moniData.Utils.TrafficEventUtils.CongestionEvent.class),t));
                        }
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("HBase操作异常: " + e.getMessage());
            // 可选择记录日志，但不抛出异常
        }
        System.out.println("congs:"+congs);

        return congs;

    }
    public static List<Pair<whu.edu.moniData.Utils.TrafficEventUtils.CongestionEvent,Long>>getCongestionEvent1(String tableName, List<Long> time){

        List<Pair<whu.edu.moniData.Utils.TrafficEventUtils.CongestionEvent,Long>> congs = new ArrayList<>();
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142,100.65.38.36,100.65.38.37,100.65.38.38");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        boolean a=true;
        int i=0;
        List<String> ss=new ArrayList<>();
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
            // 检查表是否存在
            if (!isTableExists(connection, tableName)) {
                System.out.println("表 " + tableName + " 不存在");
                return congs; // 直接返回空列表
            }

            try (Table table = connection.getTable(TableName.valueOf(tableName))) {

                for(long t:time){
                    Get get = new Get(String.valueOf(t).getBytes());
                    Result result1 = table.get(get);
                    if(a) {
                        for (Cell cell : result1.rawCells()) {
                            System.out.println("Row Key: " + Bytes.toString(result1.getRow()));
                            System.out.println("Column Family: " + Bytes.toString(CellUtil.cloneFamily(cell)));
                            System.out.println("Column Qualifier: " + Bytes.toString(CellUtil.cloneQualifier(cell)));
                            System.out.println("Value: " + Bytes.toString(CellUtil.cloneValue(cell)));
                            ss.add("Row Key: " + Bytes.toString(result1.getRow()) + "Value: " + Bytes.toString(CellUtil.cloneValue(cell))+"Column Family: " + Bytes.toString(CellUtil.cloneFamily(cell))+"Column Qualifier: " + Bytes.toString(CellUtil.cloneQualifier(cell)));
                            a=false;
                            try (BufferedWriter writer1 = new BufferedWriter(new FileWriter("/home/ljj/jiaotou/test.txt",true))) {
                                writer1.write("Row Key: " + Bytes.toString(result1.getRow()) + "Value: " + Bytes.toString(CellUtil.cloneValue(cell))+"Column Family: " + Bytes.toString(CellUtil.cloneFamily(cell))+"Column Qualifier: " + Bytes.toString(CellUtil.cloneQualifier(cell)));
                                writer1.write(System.lineSeparator());
                            }
                        }
                    }
                    get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("congestions"));
                    Result result = table.get(get);

                    byte[] valueBytes = result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("congestions"));
                    if (valueBytes != null) {
                        String value = Bytes.toString(valueBytes);
                        JSONArray objects = JSON.parseArray(value);
                        for (Object object : objects) {
                            congs.add(new Pair<>(JSON.parseObject(object.toString(), whu.edu.moniData.Utils.TrafficEventUtils.CongestionEvent.class),t));
                            i++;
                        }
                    } else {
                        System.out.println("列 congestions 不存在或值为空");
                    }
                }

            }
        } catch (IOException e) {
            System.err.println("HBase操作异常: " + e.getMessage());
            // 可选择记录日志，但不抛出异常
        }
        System.out.println("congs:"+congs);
        return congs;

    }
    public static void getByRowkey(String tableName,String rowkey) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142,100.65.38.36,100.65.38.37,100.65.38.38");  // Zookeeper 地址
        conf.set("hbase.zookeeper.property.clientPort", "2181");  // Zookeeper 端口
        try (Connection connection = ConnectionFactory.createConnection(conf);
             Table table = connection.getTable(TableName.valueOf(tableName))) {
            Get get = new Get(Bytes.toBytes(rowkey));
            Result result = table.get(get);
            for (Cell cell : result.rawCells()) {
                System.out.println("Row Key: " + Bytes.toString(result.getRow()));
                System.out.println("Column Family: " + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("Column Qualifier: " + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("Value: " + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    // 检查表是否存在的工具方法
    private static boolean isTableExists(Connection connection, String tableName) throws IOException {
        try (Admin admin = connection.getAdmin()) {
            return admin.tableExists(TableName.valueOf(tableName));
        }
    }
    // cf  VehicleSegments
    public static List<whu.edu.moniData.Utils.TrafficEventUtils.VehicleSeg> getVeByRowkey(String tableName, String rowkey) {
        List<whu.edu.moniData.Utils.TrafficEventUtils.VehicleSeg> vehicleSegs = new ArrayList<>();
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142,100.65.38.36,100.65.38.37,100.65.38.38");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        try (Connection connection = ConnectionFactory.createConnection(conf)) {
            // 检查表是否存在
            if (!isTableExists(connection, tableName)) {
                System.out.println("表 " + tableName + " 不存在");
                return vehicleSegs; // 直接返回空列表
            }

            try (Table table = connection.getTable(TableName.valueOf(tableName))) {
                Get get = new Get(Bytes.toBytes(rowkey));
                get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("VehicleSegments"));
                Result result = table.get(get);

                byte[] valueBytes = result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("VehicleSegments"));
                if (valueBytes != null) {
                    String value = Bytes.toString(valueBytes);
                    JSONArray objects = JSON.parseArray(value);
                    for (Object object : objects) {
                        vehicleSegs.add(JSON.parseObject(object.toString(), whu.edu.moniData.Utils.TrafficEventUtils.VehicleSeg.class));
                    }
                } else {
                    System.out.println("列 cf/VehicleSegments 不存在或值为空");
                }
            }
        } catch (IOException e) {
            System.err.println("HBase操作异常: " + e.getMessage());
            // 可选择记录日志，但不抛出异常
        }
        return vehicleSegs;
    }
    public static void createOrDis(Admin admin,String table,HTableDescriptor hTableDescriptor) throws IOException {
        if(admin.tableExists(TableName.valueOf(table))){
            System.out.println("Table already exists!");
        }else {
            admin.createTable(hTableDescriptor);
        }
    }

    public static void createTable(Configuration conf,String tableName,String cf) throws IOException {
        try(Connection connection =ConnectionFactory.createConnection(conf);Admin admin = connection.getAdmin()){
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            tableDescriptor.addFamily(new HColumnDescriptor(cf).setCompressionType(Compression.Algorithm.NONE));
            System.out.println("Creating table " + tableName+"...");
            createOrDis(admin,tableName,tableDescriptor);
            System.out.println("Done.");
        }
    }
    public static void deleteTable(Configuration conf,String tableName,String cf) throws IOException {
        try(Connection connection =ConnectionFactory.createConnection(conf);Admin admin = connection.getAdmin()){
            TableName table = TableName.valueOf(tableName);
            //停用表
            admin.disableTable(table);
            //删除列族
            admin.deleteColumn(table,cf.getBytes(StandardCharsets.UTF_8));
            //删除表
            admin.deleteTable(table);
        }
    }
    public static void adadColumnFamily(Configuration conf,String columnFamilyName,String tableName) throws IOException {
        try(Connection connection =ConnectionFactory.createConnection(conf) ;Admin admin = connection.getAdmin();){
            HColumnDescriptor hcd = new HColumnDescriptor(columnFamilyName);
            admin.addColumn(TableName.valueOf(tableName), hcd);
        }
    }
    public static void deleteByRowkey(Configuration conf,String columnFamilyName,String tableName) throws IOException {
        try(Connection connection =ConnectionFactory.createConnection(conf);Table table = connection.getTable(TableName.valueOf(tableName))){
            Delete delete = new Delete(Bytes.toBytes(columnFamilyName));
        }
    }
    public static void putLine(Configuration conf,String columnFamilyName,String tableName,String row1,String qualifier,String value) throws IOException {
        try(Connection connection =ConnectionFactory.createConnection(conf);Table table = connection.getTable(TableName.valueOf(tableName))){
            Put put = new Put(Bytes.toBytes(row1));//1001
            put.addColumn(Bytes.toBytes(columnFamilyName),Bytes.toBytes(qualifier),Bytes.toBytes(value));
            System.out.println("one put succeed: tableNane - "+tableName+"  rowkey - "+row1+" com family - "+columnFamilyName+" qualifier - "+qualifier+"  value - "+value);
            table.put(put);
        }
    }
    public static void putManyLines(Configuration conf,
                                    String columnFamilyName,
                                    String tableName,
                                    String row1,
                                    String qualifier,
                                    String value,
                                    String qualifier2,
                                    String value2 ) throws IOException {
        try(Connection connection =ConnectionFactory.createConnection(conf);Table table = connection.getTable(TableName.valueOf(tableName))){
            Put put = new Put(Bytes.toBytes(row1));//1001
            put.addColumn(Bytes.toBytes(columnFamilyName),Bytes.toBytes(qualifier),Bytes.toBytes(value)).addColumn(Bytes.toBytes(columnFamilyName),Bytes.toBytes(qualifier2),Bytes.toBytes(value2));
            System.out.println("two put succeed"+value+"     "+value2);
            table.put(put);
        }

    }

}
