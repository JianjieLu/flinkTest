package whu.edu.ljj.flink.merge.Version1;



import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import lombok.*;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;


/**
 * 已经更新到3.25版本的了
 */

public class Utils {

    /**
     * TimeBucket 分“桶”缓存光栅数据
     * 只是一个方便索引查询缓存数据的结构
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    @Setter
    public static class TimeBucket implements Serializable{
        private long startTime; // 时间窗口起点（秒级）
        private List<JSONObject> data; // 窗口内的数据

        // Getters and Setters
    }

    /**
     * TrajeData 是对应光栅推送的数据
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    @Setter
    public static class TrajeData implements Serializable{
        private int SN;
        private String DEVICEIP;
        private long TIME;
        private int COUNT;
        private List<TrajePoint> TDATA;
    }

    /**
     * TrajePoint 为 TrajeData 中 TDATA的点
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    @Setter
    public static class TrajePoint implements Serializable{
        private long ID;
        private String Carnumber;
        private byte Type;
        private int[] Scope;
        private float Speed;
        private byte Wayno;
        private int Tpointno;
        private byte Boolean;
        private byte Direct;
    }

    /**
     * PathTData 为交投要求返回数据的简化版本
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    @Setter
    public static class PathTData implements Serializable{
        private int pathNum;
        private long time;
        private String timeStamp;
        //        private String waySectionId;
//        private String waySectionName;
        private List<PathPoint> pathList;
    }

    /**
     * PathPoint 为 PathData中 pathList 存储的点的简化版本
     * 注意：time是多余的，原本只要求timestamp
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    @Setter
    public static class PathPoint implements Serializable{
        private int direction;
        private long id;
        private int laneNo;
        private int mileage;
        private String plateNo = "";
        private float speed;
        private String timeStamp;
        private Integer plateColor = null;
        private Integer vehicleType = null;
        private double longitude;
        private double latitude;
        private double carAngle;
        // 现在没有桩号
        private String stakeId = "";
        private Integer originalType = null;
        private Integer originalColor = null;
        private String specialFlag = "";
    }

    /**
     * VehicleMapping 存储历史匹配记录
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    @Setter
    public static class VehicleMapping implements Serializable{
        private Map<String, Integer> plateCounts = new HashMap<>(); // 车牌号 -> 匹配次数
        // 最后更新时间，这个本意是想判断无牌车的，不知道是否能用来记录去服务区
        // 目前应该可以用来
        private long lastUpdateTime = 0;
        // 通过输出“默A00000”的次数来判断无牌车
        // 待删除
        private int defaultPlateSum = 0;
        private String lastMMPlate = "";
        private Integer originType = null;
        private Integer originColor = null;
        private Integer plateColor = null;
        private Integer VehicleType = null;
        // 构造函数、Getter 和 Setter
        public Pair<String, Integer> getMostMatchedPlate()
        {
            String mmPlate = "";
            int mmNum = 0;
            // 极端情况，每次匹配都有新车牌，但是为了效率，只考虑当前最多的
            if(!plateCounts.isEmpty()) {
                for (Map.Entry<String, Integer> entry : plateCounts.entrySet()) {
                    if (entry.getValue() > mmNum) {
                        mmPlate = entry.getKey();
                        mmNum = entry.getValue();
                    }
                }
            }
            return Pair.of(mmPlate, mmNum);
        }
    }

    /**
     * GantryData 存储解析后的门架数据
     */
    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    @Setter
    public static class GantryData implements Serializable {
        private String id = null;
        private String envState = null;
        private String uploadTime = null;
        private String plateNumber = null;
        private String tollPlateNumber = null;
        private int headLaneCode = 0; // >=1有效
        private int direction = 0; // 1和2有效
        private int mileage = -1; // > 0有效
        // 一定有plateColor值
        private int plateColor;
        private Integer tollPlateColor = null;
        private Integer tollFeeVehicleType = null;
        // 这里不确定是不是按照标准的值，先赋值试试
        private Integer tollVehicleUserType = null;

        @Override
        public String toString() {
            return JSON.toJSONString(this);
        }
    }


    /**
     * GanryInfo 存储门架固定信息
     * 孝汉应因为只有两个，所以当时只在代码里判断，正常情况已知数据应该先加载的
     * 这样回头扩展Gantry的JSON数据的时候可以直接用这里的
     * 关于Gantry的数据还用直接解析成POJO类吗
     */
    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    @Setter
    public static class GantryInfo implements Serializable{
        private String id;
        private int mileage;
        private int direction;
        @Override
        public String toString() {
            return "GantryInfo{" +
                    "id='" + id + '\'' +
                    ", mileage=" + mileage +
                    ", direction=" + direction +
                    '}';
        }
    }

    /**
     * GantryAssignment 会在主程序的static代码块中加载已知的gantry信息
     */
    @Getter
    public static class GantryAssignment implements Serializable {
        private Map<Integer, List<GantryInfo>> gantriesByDirection;
        private Map<String, GantryInfo> gantriesByID;

        public GantryAssignment(String excelFilePath) throws IOException {
            // 从Excel文件加载卡口信息
            Pair<Map<Integer, List<GantryInfo>>, Map<String, GantryInfo>> result = loadCheckpointsFromExcel(excelFilePath);
            this.gantriesByDirection = result.getLeft();
            this.gantriesByID = result.getRight();
        }

        private Pair<Map<Integer, List<GantryInfo>>, Map<String, GantryInfo>> loadCheckpointsFromExcel(String filePath) throws IOException {
            Map<Integer, List<GantryInfo>> gantriesByDirection = new HashMap<>();
            Map<String, GantryInfo> gantriesByID = new HashMap<>();
            FileInputStream fis = new FileInputStream(filePath);
            Workbook workbook = new XSSFWorkbook(fis);
            Sheet sheet = workbook.getSheetAt(0);

            for (Row row : sheet) {
                String id = row.getCell(0).getStringCellValue();
                int mileage = (int) row.getCell(3).getNumericCellValue();
                int direction = (int) row.getCell(5).getNumericCellValue();
                GantryInfo gantry = new GantryInfo(id, mileage, direction);
                List<GantryInfo> sideGantries;
                if (!gantriesByDirection.containsKey(direction))
                    sideGantries = new ArrayList<>();
                else
                    sideGantries = new ArrayList<>(gantriesByDirection.get(direction));

                sideGantries.add(gantry);
                gantriesByDirection.put(direction, sideGantries);
                gantriesByID.put(id, gantry);
            }

            workbook.close();
            fis.close();
            return Pair.of(gantriesByDirection, gantriesByID);
        }

        public int assignGantry(PathPoint ppoint) {
            if (gantriesByDirection.isEmpty()) {
                return 0;
            }

            int vehicleMileage = ppoint.getMileage();

            // 将 Map 转换为按里程排序的列表
            List<GantryInfo> sortedGantries = new ArrayList<>(gantriesByDirection.get(ppoint.getDirection()));
            sortedGantries.sort(Comparator.comparingInt(GantryInfo::getMileage));
            System.out.println(sortedGantries);

            // 使用二分查找法找到最接近的卡口
            int index = binarySearchClosest(sortedGantries, vehicleMileage);
            System.out.println("index："+index);
            if (index < 0) {
                return 0;
            }

            GantryInfo closest = sortedGantries.get(index);
            int distance = Math.abs(closest.getMileage() - vehicleMileage);

            if (distance <= 100) { // 100米
                return closest.getMileage();
            }
            return 0;
        }

        private int binarySearchClosest(List<GantryInfo> sortedList, int targetMileage) {
            int left = 0;
            int right = sortedList.size() - 1;
            if(left == right)
                return 0;

            while (left <= right) {
                int mid = left + (right - left) / 2;
                int midMileage = sortedList.get(mid).getMileage();

                if (Math.abs(midMileage - targetMileage) <= 50) {
                    return mid;
                } else if (midMileage < targetMileage) {
                    left = mid + 1;
                } else {
                    right = mid - 1;
                }
            }

            // 检查左侧和右侧哪个更接近目标里程
            int closestIndex = left;
            if (closestIndex > 0) {
                int leftDistance = Math.abs(sortedList.get(closestIndex - 1).getMileage() - targetMileage);
                int rightDistance = (closestIndex < sortedList.size()) ?
                        Math.abs(sortedList.get(closestIndex).getMileage() - targetMileage) : Integer.MAX_VALUE;
                if (rightDistance < leftDistance) {
                    closestIndex = right;
                }
            }

            return closestIndex;
        }
    }


    public static long convertToTimestamp(String dateTimeStr) {
        // 定义日期时间格式
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        // 将字符串转换为 LocalDateTime 对象
        LocalDateTime dateTime = LocalDateTime.parse(dateTimeStr, formatter);

        // 将 LocalDateTime 转换为时间戳（long 类型）
        // 如果需要考虑时区，可以使用 ZonedDateTime 并指定时区
        long timestamp = dateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();

        return timestamp;
    }

    public static String convertToTimestampString(long timestamp) {
        // 定义日期时间格式
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        // 将时间戳转换为 Instant 对象
        Instant instant = Instant.ofEpochMilli(timestamp);

        // 将 Instant 转换为 LocalDateTime（考虑系统默认时区）
        LocalDateTime dateTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());

        // 格式化为字符串
        String dateTimeStr = dateTime.format(formatter);

        return dateTimeStr;
    }

    public static long convertToTimestampMillis(String dateTimeStr) {
        // 定义日期时间格式
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS");

        // 将字符串转换为 LocalDateTime 对象
        LocalDateTime dateTime = LocalDateTime.parse(dateTimeStr, formatter);

        // 将 LocalDateTime 转换为时间戳（long 类型）
        // 如果需要考虑时区，可以使用 ZonedDateTime 并指定时区
        long timestamp = dateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();

        return timestamp;
    }

    public static String convertFromTimestampMillis(long timestamp) {
        // 定义日期时间格式
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS");

        // 将时间戳转换为 Instant 对象
        Instant instant = Instant.ofEpochMilli(timestamp);

        // 将 Instant 转换为 LocalDateTime（考虑系统默认时区）
        LocalDateTime dateTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());

        // 格式化为字符串
        String dateTimeStr = dateTime.format(formatter);

        return dateTimeStr;
    }
}
