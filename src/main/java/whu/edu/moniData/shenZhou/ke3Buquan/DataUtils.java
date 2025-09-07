package whu.edu.moniData.shenZhou.ke3Buquan;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import lombok.*;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static com.sun.xml.bind.v2.util.ClassLoaderRetriever.getClassLoader;

public class DataUtils {

    /**
     * TimeBucket 分“桶”缓存光栅数据
     * 只是一个方便索引查询缓存数据的结构
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    @Setter
    public static class TimeBucket implements Serializable {
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
    public static class TrajeData implements Serializable {
        private int SN;
        private String DEVICEIP;
        private long TIME;
        private int COUNT;
        private List<TrajePoint> TDATA;
        private int SegId;
    }

    /**
     * TrajePoint 为 TrajeData 中 TDATA的点
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    @Setter
    public static class TrajePoint implements Serializable {
        private long ID;
        private String Carnumber;
        private byte Type;
        private int[] Scope;
        private double speed;
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
    public static class PathTData implements Serializable {
        private int pathNum;
        private long time;
        private String timeStamp;
        private Integer segId;
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
    public static class PathPoint implements Serializable {
        private Integer direction;
        private long id;
        private int laneNo;
        private Double mileage;
        private String plateNo = "";
        private Double speed;
        private String timeStamp;
        private Integer plateColor = null;
        private Integer vehicleType = null;
        private double longitude;
        private double latitude;
        private double carAngle;
        // 现在没有桩号 -> 现在有了
        private String stakeId = "";
        private Integer originalType = null;
        private Integer originalColor = null;
        private String specialFlag = "";
        private String ramp = "";
    }

    /**
     * VehicleMapping 存储历史匹配记录
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    @Setter
    public static class VehicleMapping implements Serializable {
        private Map<String, Integer> plateCounts = new HashMap<>(); // 车牌号 -> 匹配次数
        // 最后更新时间，这个本意是想判断无牌车的，不知道是否能用来记录去服务区 - before 4.1
        // 目前应该可以用来防止车辆重复匹配
        private long lastUpdateTime = 0;
        // 通过输出“默A00000”的次数来判断无牌车
        // 待删除
//        private int defaultPlateSum = 0;
        private String lastMMPlate = "";
        private Integer originType = null;
        private Integer originColor = null;
        private Integer plateColor = null;
        private Integer VehicleType = null;

        // 构造函数、Getter 和 Setter
        public Pair<String, Integer> getMostMatchedPlate() {
            String mmPlate = "";
            int mmNum = 0;
            // 极端情况，每次匹配都有新车牌，但是为了效率，只考虑当前最多的
            if (!plateCounts.isEmpty()) {
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
        private String deviceId = null;
        private String envState = null;
        private String uploadTime = null;
        private String plateNumber = null;
        private String tollPlateNumber = null;
        private int headLaneCode = 0; // >=1有效
        private int direction = 0; // 1和2有效
        private double mileage = -1.0; // > 0有效
        // 一定有plateColor值
        private int plateColor;
        private Integer tollPlateColor = null;
        private Integer tollFeeVehicleType = null;
        // 这里不确定是不是按照标准的值，先赋值试试
        private Integer tollVehicleUserType = null;
        // 分区
        private int segId;

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
    public static class GantryInfo implements Serializable {
        private String id;
        private double mileage;
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

//    /**
//     * GantryAssignment 会在主程序的static代码块中加载已知的gantry信息
//     */
//    @Getter
//    @Setter
//    @NoArgsConstructor
//    @AllArgsConstructor
//    public static class GantryAssignment implements Serializable {
//        private Map<Integer, List<GantryInfo>> gantriesByDirection;
//        private Map<String, GantryInfo> gantriesByID;
//        private Map<String, Integer> gantrySegAssign;
//
//        public GantryAssignment(String excelFilePath) throws IOException {
//            // 从Excel文件加载卡口信息
//            Tuple3<Map<Integer, List<GantryInfo>>, Map<String, GantryInfo>, Map<String, Integer>> result = loadCheckpointsFromExcel(excelFilePath);
//            this.gantriesByDirection = result.getField(0);
//            this.gantriesByID = result.getField(1);
//            this.gantrySegAssign = result.getField(2);
//        }
//
//        private Tuple3<Map<Integer, List<GantryInfo>>, Map<String, GantryInfo>, Map<String, Integer>> loadCheckpointsFromExcel(String filePath) throws IOException {
//            Map<Integer, List<GantryInfo>> gantriesByDirection = new HashMap<>();
//            Map<String, GantryInfo> gantriesByID = new HashMap<>();
//            Map<String, Integer> gantrySegAssign = new HashMap<>();
//
//            // 使用ClassLoader获取资源流
//            try (InputStream is = getClass().getClassLoader().getResourceAsStream(filePath)) {
//                if (is == null) {
//                    System.out.println("文件未找到: " + filePath);
//                }
//
//                Workbook workbook = new XSSFWorkbook(is);
//                Sheet sheet = workbook.getSheetAt(0);
//
//                // 跳过表头
//                // 跳过最后一个卡口，因为最后一个卡口超过了里程范围
//                for (int i = 1; i < sheet.getLastRowNum(); i++) {
//                    Row row = sheet.getRow(i);
//
//                    String id = row.getCell(0).getStringCellValue();
//                    double mileage = (int) row.getCell(3).getNumericCellValue();
//                    int direction = (int) row.getCell(5).getNumericCellValue();
//                    int segId = (int) row.getCell(7).getNumericCellValue();
//                    GantryInfo gantry = new GantryInfo(id, mileage, direction);
//
//                    gantrySegAssign.put(id, segId);
//
//                    List<GantryInfo> sideGantries = gantriesByDirection
//                            .computeIfAbsent(direction, k -> new ArrayList<>());
//                    sideGantries.add(gantry);
//                    gantriesByID.put(id, gantry);
//                }
//
//                workbook.close();
//            }
//            return Tuple3.of(gantriesByDirection, gantriesByID, gantrySegAssign);
//        }
//
//        public int assignGantry(PathPoint ppoint) {
//            if (gantriesByDirection.isEmpty()) {
//                return 0;
//            }
//
//            int vehicleMileage = ppoint.getMileage();
//
//            // 将 Map 转换为按里程排序的列表
//            List<GantryInfo> sortedGantries = new ArrayList<>(gantriesByDirection.get(ppoint.getDirection()));
//            sortedGantries.sort(Comparator.comparingInt(GantryInfo::getMileage));
//            // 5.7 这里是输出所有的排序情况，仅作为调试用，但这里其实只有1个卡口，目前先注释
////            System.out.println(sortedGantries);
//
//            // 使用二分查找法找到最接近的卡口
//            int index = binarySearchClosest(sortedGantries, vehicleMileage);
//
//            System.out.println("index：" + index);
//            if (index >= 0)
//                System.out.println("匹配卡口：" + sortedGantries.get(index));
//                // 5.7 待修改，这里不应该有超越边界的情况 -> 小于最小，大于最大的
//            else {
//                System.out.println("超过最小边界，匹配卡口mileage为0，即无匹配卡口");
//                return 0;
//            }
////            System.out.println("匹配卡口："+sortedGantries.get(index));
//
//            GantryInfo closest = sortedGantries.get(index);
//            int distance = Math.abs(closest.getMileage() - vehicleMileage);
//
//            if (distance <= 50) { // 100米 -> 50米
//                return closest.getMileage();
//            }
//            return 0;
//        }
//
//        private int binarySearchClosest(List<GantryInfo> sortedList, int targetMileage) {
//            int left = 0;
//            int right = sortedList.size() - 1;
//            if (left == right)
//                return 0;
//
//            while (left <= right) {
//                int mid = left + (right - left) / 2;
//                int midMileage = sortedList.get(mid).getMileage();
//
//                if (Math.abs(midMileage - targetMileage) <= 50) {
//                    return mid;
//                } else if (midMileage < targetMileage) {
//                    left = mid + 1;
//                } else {
//                    right = mid - 1;
//                }
//            }
//
//            // 边界处理
//            int closestIndex = left;
//
//            // 其实我们发现，当点落在非匹配范围内时，具体离哪一个近都不重要，这里省略那一步
//
//            if (closestIndex == -1)
//                closestIndex = 0;
//            else if (closestIndex == sortedList.size())
//                closestIndex = sortedList.size() - 1;
//
//            return closestIndex;
//        }
//    }

    /**
     * 记录确认匹配好的车辆信息
     */
    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class VehicleInfo implements Serializable {
        private String plateNo;
        private Integer plateColor;
        private Integer vehicleType;
        private Integer originalType;
        private Integer originalColor;
        private String specialFlag = "";
        private Long fineMatchTime;
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

    public static String convertToTimestampMillisString(long timestamp) {
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

    /**
     * StakeInfo
     */
    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    public static class StakeInfo implements Serializable {
        private String stake;
        private double[] lnglat;
    }

    /**
     * StakeAssignment 会在主程序的static代码块中加载已知的gantry信息
     */
    @Getter
    @Setter
    public static class MileageConverter implements Serializable {
        private Map<Double, StakeInfo> stakeInfoMap;
        private List<Double> mileageList;

        public MileageConverter(String excelFilePath) throws IOException {
            // 从Excel文件加载卡口信息
            this.stakeInfoMap = loadCheckpointsFromJSON(excelFilePath);
            this.mileageList = loadSortedList(this.stakeInfoMap);
        }

        private Map<Double, StakeInfo> loadCheckpointsFromJSON(String filePath) throws IOException {
            // 读取JSON
            String jsonString = readFileContent(filePath);

            // 解析 JSON 数据为 List<StakeInfo>
            List<StakeInfo> stakeInfoList = com.alibaba.fastjson2.JSON.parseArray(jsonString, StakeInfo.class);

            Map<Double, StakeInfo> stakeInfoMap = new HashMap<>();

            for(StakeInfo stakeInfo : stakeInfoList)
                stakeInfoMap.put(stakeToMileage(stakeInfo.getStake()), stakeInfo);

            return stakeInfoMap;
        }

        private List<Double> loadSortedList(Map<Double, StakeInfo> stakeInfoMap) {
            List<Double> mileageList = new ArrayList<>(stakeInfoMap.keySet());
            mileageList.sort(Double::compare);
            return mileageList;
        }

        public StakeInfo findCoordinate(double targetMileage) {
            if(targetMileage < mileageList.get(0))
                return stakeInfoMap.get(mileageList.get(0));
            else if(targetMileage > mileageList.get(mileageList.size() - 1))
                return stakeInfoMap.get(mileageList.get(mileageList.size() - 1));
            return stakeInfoMap.get(targetMileage);
        }

        private String readFileContent(String filePath) {
            try (InputStream is = getClassLoader().getResourceAsStream(filePath);
                 BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {

                if (is == null) {
                    throw new FileNotFoundException("文件未找到: " + filePath);
                }

                StringBuilder sbuilder = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    sbuilder.append(line);
                }

                return sbuilder.toString();
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        }

        private double stakeToMileage(String stakeId) {
            return Double.parseDouble(stakeId.split("\\+")[0].substring(1)) * 1000 + Double.parseDouble(stakeId.split("\\+")[1]);
        }
    }

    /**
     * BaseStationData
     */
    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    public static class BaseStationData {
        Integer deviceId;
        Long timestampMicrosec;
        Integer participantCount;
        List<BSPoint> participants;
    }

    /**
     * BSPoint
     */
    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    public static class BSPoint {
        Integer id;
        Integer type;
        Integer color;
        Integer source;
        Integer cameraId;
        String rampStake = "";
        Double mileage;
        String plateNo = "默A00000";
        Integer laneNo;
        Double longitude;
        Double latitude;
        Float altitude;
        double speed;
        Float heading;
//        Float length;
//        Float width;
//        Float height;
    }

    /**
     * Location
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    @Setter
    public static class Location {
        String ramp;
        Integer laneNum;
        String location;
        double locationNum;
        double longitude;
        double latitude;
        Integer laneType;
        Integer direction;
    }

    /**
     * StakeAssignment 会在主程序的static代码块中加载已知的gantry信息
     */
    @Getter
    @Setter
    public static class StakeAssignment <T> implements Serializable {
        private static final double EARTH_RADIUS = 6371.393;
        @Getter
        private final Class<T> type;
        @Getter
        private final int laneTotalNum;
        // 具体到每个车道
        private List<List<T>> stakeInfoLaneLists;

        public StakeAssignment(Class<T> type, String jsonFilePath, Integer laneTotalNum) throws IOException {
            // 加载组成List的具体的类的类型，从而绕过泛型擦除
            this.type = type;
            // 获取所读取的json表有几个车道
            this.laneTotalNum = laneTotalNum;
            // 从
            this.stakeInfoLaneLists = loadCheckpointsFromJSON(jsonFilePath);
        }

        private List<List<T>> loadCheckpointsFromJSON(String filePath) throws IOException {
            // 读取JSON
            String jsonString = readFileContent(filePath);

            // 初始化结果List
            List<List<T>> stakeInfoLaneLists = new ArrayList<>();
            for(int i = 0; i < this.laneTotalNum; i++)
                stakeInfoLaneLists.add(new ArrayList<>());

            // 解析 JSON 数据为 List<StakeInfo>
            List<T> stakeInfoList = JSON.parseArray(jsonString, type);

            if(type == Location.class) {
                if (stakeInfoList != null) {
                    for(T location : stakeInfoList) {
                        if(location instanceof Location)
                            stakeInfoLaneLists.get(((Location)location).getLaneNum() - 1).add(location);
                    }
                    // 按里程/桩号从小到大
                    for(List<T> list : stakeInfoLaneLists)
                        list.sort(Comparator.comparingDouble(o -> ((Location) o).getLocationNum()));
                }
            }
            // 主路的待定
//            else if(type == StakeInfo.class) {
//                if (stakeInfoList != null) {
//                    for(T stakeInfo : stakeInfoList) {
//                        stakeInfoLaneLists.get(((StakeInfo)stakeInfo).getLaneNum() - 1).add(stakeInfo);
//                    }
//                }
//            }

            return stakeInfoLaneLists;
        }

        public Location findNearestStake(double tarLng, double tarLat) {

            if(type == Location.class) {
                List<Location> optionalLocations = new ArrayList<>();
                for(List<T> locationList : stakeInfoLaneLists) {

                    int left = 0;
                    int right = locationList.size() - 1;
                    Location midLocation;
                    boolean flag = false;

                    while (left <= right) {
                        int mid = left + (right - left) / 2;
                        midLocation = ((Location)locationList.get(mid));
                        double midLng = midLocation.getLongitude();
                        double midLat = midLocation.getLatitude();
                        double midDistance = calculateDistance(midLng, midLat, tarLng, tarLat) * 1000;
                        if (midDistance <= 0.25) {
                            System.out.println("最小距离："+midDistance);
                            return midLocation;
//                            optionalLocations.add(midLocation);
//                            flag = true;
//                            break;
                        } else if ((Arrays.asList("A", "B").contains(midLocation.getRamp()) && crossProduct(114.044532-midLng, 30.918772-midLat, tarLng-114.044532, tarLat-30.918772) > 0) ||
                                    (Arrays.asList("C", "D").contains(midLocation.getRamp()) && tarLat > midLat ))
                        // 北半球
                        {
                            right = mid - 1;
                        } else {
                            left = mid + 1;
                        }
                    }
//                    if(!flag) {
                        midLocation = ((Location)locationList.get(Math.min(left, locationList.size()-1)));
                        optionalLocations.add(midLocation);
//                    }
                }

                System.out.println("所有的optionalLocations："+optionalLocations);

                // 可适当剪枝 / 可以空间换时间 -> 再写个类
                Location resltLocation = optionalLocations.stream().min(Comparator.comparingDouble(o ->
                        calculateDistance(o.getLongitude(), o.getLatitude(), tarLng, tarLat) * 1000)).get();
                System.out.println("最近点的距离："+calculateDistance(resltLocation.getLongitude(), resltLocation.getLatitude(), tarLng, tarLat) * 1000);
                if(calculateDistance(resltLocation.getLongitude(), resltLocation.getLatitude(), tarLng, tarLat) * 1000 <= 0.25)
                    return resltLocation;
                else
                    return null;
            }
            else if(type == StakeInfo.class) {
//                int left = 0;
//                int right = stakeInfoList.size() - 1;
//
//                while (left <= right) {
//                    int mid = left + (right - left) / 2;
//                    double[] coordinate = stakeInfoList.get(mid).getLnglat();
//                    double midDistance = calculateDistance(coordinate[0], coordinate[1], lng, lat) * 1000;
//
//                    if (midDistance <= 0.5) {
//                        return stakeInfoList.get(mid).getStake();
//                    } else if (coordinate[1] < lat) {
//                        right = mid - 1;
//                    } else {
//                        left = mid + 1;
//                    }
//                }
//                return stakeInfoList.get(Math.min(left, stakeInfoList.size()-1)).getStake();
            }

            return null;
        }

        public static double calculateDistance(double lon1, double lat1, double lon2, double lat2) {
            // 将角度转换为弧度
            double dLat = Math.toRadians(lat2 - lat1);
            double dLon = Math.toRadians(lon2 - lon1);
            lat1 = Math.toRadians(lat1);
            lat2 = Math.toRadians(lat2);

            // Haversine 公式
            double a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
                    Math.cos(lat1) * Math.cos(lat2) *
                            Math.sin(dLon / 2) * Math.sin(dLon / 2);
            double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
            double distance = EARTH_RADIUS * c;

            return distance;
        }

        public static double crossProduct(double midLng, double midLat, double tarLng, double tarLat) {
            // x1*y2 - x2*y1
//            return midLat * tarLng - tarLat * midLng;
            return midLng * tarLat - tarLng * midLat;
        }

        private String readFileContent(String filePath) {
            try (InputStream is = getClassLoader().getResourceAsStream(filePath);
                 BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {

                if (is == null) {
                    throw new FileNotFoundException("文件未找到: " + filePath);
                }

                StringBuilder sbuilder = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    sbuilder.append(line);
                }

                return sbuilder.toString();
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        }

        private double stakeToMileage(String stakeId) {
            return Double.parseDouble(stakeId.split("\\+")[0].substring(1)) * 1000 + Double.parseDouble(stakeId.split("\\+")[1]);
        }
    }

    /**
     * StakeAssignment 会在主程序的static代码块中加载已知的gantry信息
     */
    @Getter
    @Setter
    public static class RampStakeAssignment implements Serializable {
        private static final double EARTH_RADIUS = 6371.393;

        @Getter
        private final int laneTotalNum;
        // 具体到每个车道
        private List<List<Location>> stakeInfoLaneLists;

        public RampStakeAssignment(String jsonFilePath, Integer laneTotalNum) throws IOException {
            // 获取所读取的json表有几个车道
            this.laneTotalNum = laneTotalNum;
            // 从json文件中获取
            this.stakeInfoLaneLists = loadCheckpointsFromJSON(jsonFilePath);
        }

        private List<List<Location>> loadCheckpointsFromJSON(String filePath) throws IOException {
            // 读取JSON
            String jsonString = readFileContent(filePath);

            // 初始化结果List
            List<List<Location>> stakeInfoLaneLists = new ArrayList<>();
            for(int i = 0; i < this.laneTotalNum; i++)
                stakeInfoLaneLists.add(new ArrayList<>());

            // 解析 JSON 数据为 List<StakeInfo>
            List<Location> stakeInfoList = JSON.parseArray(jsonString, Location.class);

            if (stakeInfoList != null) {
                for (Location location : stakeInfoList) {
                    stakeInfoLaneLists.get(location.getLaneNum() - 1).add(location);
                    // 按里程/桩号从小到大
                    for (List<Location> list : stakeInfoLaneLists)
                        list.sort(Comparator.comparingDouble(Location::getLocationNum));
                }
            }

            return stakeInfoLaneLists;
        }

        public Location findNearestStake(double tarLng, double tarLat) {
            // 所有可能的匹配点
            List<Location> optionalLocations = new ArrayList<>();
            for (List<Location> locationList : stakeInfoLaneLists) {

                int left = 0;
                int right = locationList.size() - 1;
                Location midLocation;
                boolean flag = false;

                while (left <= right) {
                    int mid = left + (right - left) / 2;
                    midLocation = locationList.get(mid);
                    double midLng = midLocation.getLongitude();
                    double midLat = midLocation.getLatitude();
                    double midDistance = calculateDistance(midLng, midLat, tarLng, tarLat) * 1000;
                    if (midDistance <= 0.25) {
                        System.out.println("最小距离：" + midDistance);
                        return midLocation;
//                            optionalLocations.add(midLocation);
//                            flag = true;
//                            break;
                    } else if ((Arrays.asList("A", "B").contains(midLocation.getRamp()) && crossProduct(114.044532 - midLng, 30.918772 - midLat, tarLng - 114.044532, tarLat - 30.918772) > 0) ||
                            (Arrays.asList("C", "D").contains(midLocation.getRamp()) && tarLat > midLat))
                    // 北半球
                    {
                        right = mid - 1;
                    } else {
                        left = mid + 1;
                    }
                }
//                    if(!flag) {
                midLocation = locationList.get(Math.min(left, locationList.size() - 1));
                optionalLocations.add(midLocation);
//                    }
            }

            System.out.println("所有的optionalLocations：" + optionalLocations);

            // 可适当剪枝 / 可以空间换时间 -> 再写个类
            Location resltLocation = optionalLocations.stream().min(Comparator.comparingDouble(o ->
                    calculateDistance(o.getLongitude(), o.getLatitude(), tarLng, tarLat) * 1000)).get();
            System.out.println("最近点的距离：" + calculateDistance(resltLocation.getLongitude(), resltLocation.getLatitude(), tarLng, tarLat) * 1000);
            if (calculateDistance(resltLocation.getLongitude(), resltLocation.getLatitude(), tarLng, tarLat) * 1000 <= 0.25)
                return resltLocation;
            else
                return null;
        }

        public static double calculateDistance(double lon1, double lat1, double lon2, double lat2) {
            // 将角度转换为弧度
            double dLat = Math.toRadians(lat2 - lat1);
            double dLon = Math.toRadians(lon2 - lon1);
            lat1 = Math.toRadians(lat1);
            lat2 = Math.toRadians(lat2);

            // Haversine 公式
            double a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
                    Math.cos(lat1) * Math.cos(lat2) *
                            Math.sin(dLon / 2) * Math.sin(dLon / 2);
            double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
            double distance = EARTH_RADIUS * c;

            return distance;
        }

        public static double crossProduct(double midLng, double midLat, double tarLng, double tarLat) {
            // x1*y2 - x2*y1
//            return midLat * tarLng - tarLat * midLng;
            return midLng * tarLat - tarLng * midLat;
        }

        private static String readFileContent(String filePath) {
            try (InputStream is = getClassLoader().getResourceAsStream(filePath);
                 BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {

                if (is == null) {
                    throw new FileNotFoundException("文件未找到: " + filePath);
                }

                StringBuilder sbuilder = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    sbuilder.append(line);
                }

                return sbuilder.toString();
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        }

        private double stakeToMileage(String stakeId) {
            return Double.parseDouble(stakeId.split("\\+")[0].substring(1)) * 1000 + Double.parseDouble(stakeId.split("\\+")[1]);
        }
    }

    /**
     * BdsData
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    @Setter
    public static class BdsData {
        String positiontime;
        Integer redirect;
        Integer platecolor;
        Integer curaccesscode;
        String objectName;
        Double lon;
        Double lat;
        Double vec1;
//        Integer objectId;
        Integer trans;
        String vehicle;
        String entrance;
        String specialFlag;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    @Setter
    public static class StationData{
        int frameNum;
        String globalTime;
        long kafkaTime;
        String orgCode;
        List<StationTarget> targetList;

    }
    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    public class stat {
        int station;
        int lane;
    }
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    @Setter
    public static class StationTarget{
        float angle;
        int axisX;
        int axisY;
        int axisZ;
        int carColor;
        int carType;
        int disBefore;
        int enGap;
        String firstReceiveTime;
        int id;
        int lane;
        double lat;
        int licenseColor;
        double lon;
        String orgCode;
        int passTime;
        String picLicense;
        double speed;
        double speedAvg;
        int station;
    }

    /**
     * RampPointData
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    @Setter
    public static class SpatialPoint {
        String ramp;
        Integer laneNum;
        String location;
        double locationNum;
        double[] coordinate;
    }

    /**
     * RampBSMap
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    @Setter
    public static class RampBSMap {
        Long time = 0L;
        Map<Integer, BaseStationData> bsMap;
    }

    /**
     * RampCarTrack
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    @Setter
    public static class RampCarTrack {
        Integer id;
        String plateNo = "";
        // 如果是C，那就是C-A，同理如果是D，那就是B-D
        String ramp;
        List<Integer> bsIdSeries;
        // k为基站deviceId，v为车辆ID
        Map<Integer, Integer> bsIdMap;
        Long lastUpdateTime;
    }

    public static List<SpatialPoint> loadCheckpointsFromJSON(String filePath) throws IOException {
        // 读取JSON
        String jsonString = readFileContent(filePath);

        // 初始化结果List
        List<Location> locationList =  JSON.parseArray(jsonString, Location.class);
        List<SpatialPoint> spatialPoints = new ArrayList<>();

        if (locationList != null) {
            for (Location location : locationList) {
                double[] coordinate = new double[2];
                coordinate[0] = location.getLongitude();
                coordinate[1] = location.getLatitude();
                spatialPoints.add(new SpatialPoint(location.getRamp(), location.getLaneNum(),
                        location.getLocation(), location.getLocationNum(), coordinate));
            }
        }

        return spatialPoints;
    }

    public static String readFileContent(String filePath) {
        try (InputStream is = getClassLoader().getResourceAsStream(filePath);
             BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {

            if (is == null) {
                throw new FileNotFoundException("文件未找到: " + filePath);
            }

            StringBuilder sbuilder = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                sbuilder.append(line);
            }

            return sbuilder.toString();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static Map<Integer, Double[]> loadCheckpointsFromEXCEL(String filePath) throws IOException {
        Map<Integer, Double[]> bsLoactionMap = new HashMap<>();

        try (InputStream is = getClassLoader().getResourceAsStream(filePath)) {
            if (is == null) {
                throw new FileNotFoundException("文件未找到: " + filePath);
            }

            Workbook workbook = new XSSFWorkbook(is);

            Sheet sheet = workbook.getSheetAt(0);

            // 跳过表头
            // sheet.getLastRowNum()注意这个返回的的是最后一行的索引，且是从0开始的
            for (int i = 1; i <= sheet.getLastRowNum(); i++) {
                Row row = sheet.getRow(i);

                // 读取基站信息
                int id = (int)row.getCell(0).getNumericCellValue();
                double lon = row.getCell(5).getNumericCellValue();
                double lat = row.getCell(6).getNumericCellValue();

                // 基站中心坐标
                Double[] coordinate = new Double[]{lon, lat};

                // 写入
                bsLoactionMap.put(id, coordinate);
            }
            // 关闭资源
            workbook.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        System.out.println("初始化的bsLoactionMap是："+JSON.toJSONString(bsLoactionMap));

        return bsLoactionMap;
    }

}
