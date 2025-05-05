package whu.edu.moniData.Utils;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.TypeReference;
import lombok.*;
import com.alibaba.fastjson2.JSON;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class TrafficEventUtils {
    // 基类：交通事件类
    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    public static class TrafficEvent implements Serializable{
        private Integer eventId; // 事件id（唯一性）
        private String timeStamp; // 事件上报时间戳
        private Integer eventType; // 事件类型
        private String startStake; // 事件起始桩号
        private String endStake; // 事件结束桩号
        private Integer startMileage; // 事件范围起始里程
        private Integer endMileage; // 事件范围截止里程
        private Double startLongitude; // 事件范围起始经度
        private Double startLatitude; // 事件范围起始纬度
        private Double endLongitude; // 事件范围截止经度
        private Double endLatitude; // 事件范围截止纬度
        private String laneNo; // 事件或施工所在车道号
        private Integer direction; // 事件发生区域的行车方向
        private JSONArray carList; // 事件发生车辆集合
        private String eventLevel; // 事件等级
        private String eventDes; // 事件概要描述
        private String eventReason; // 事件原因
        private String eventPicPath; // 事件现场图片路径，多张逗号分隔
        private String eventVideoPath; // 事件现场视频路径
//        private String eventSource; // 事件源
        private String sourceRemark; // 来源备注
        private String waySectionId; // 路段id
        private String waySectionName; // 路段名称
        private Boolean manualAudit; // 事件是否需要人工审核校验
        private Integer constructionVehicles; // 施工类事件，施工车数量
        private Integer constructionPerson; // 施工类事件，施工人员数量

        // toString方法
        @Override
        public String toString() {
            return JSON.toJSONString(this);
        }
    }


    // 继承自TrafficEvent的拥堵类
    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    public static class CongestionEvent extends TrafficEvent implements Serializable{
        private Integer congestionDuration; // 拥堵时间 (/min)
        private Double congestionIndex; // 拥堵指数
        private Double averageSpeed; // 平均车速 (km/h)
        private Double congestionMileage; // 拥堵总里程，精确到小数点后一位
        private Integer busCount; // 客车数量
        private Integer truckCount; // 货车数量
        private Integer chemicalCount; // 危化车数量
        private Integer heavyTruckCount; // 重型货车数量

        // toString方法
        @Override
        public String toString() {
            return super.toString();
        }
    }

    /**
     *  VehicleSeg 表示一辆车某分钟内监测的信息
     */
    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    @Setter
    public static class VehicleSeg implements Serializable{
        private long carId;
        private String plateNo;
        private Integer plateColor;
        private float speedSum;
        private int direction;
        private int pointSum;
        private Integer originalType = null;
        private String specialFlag = null;
        private int laneNo;
//        private double averageSpeed;
    }

    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    @Setter
    public static class EventCar implements Serializable{
        private long carId;
        private String plateNo = "";
        private Integer plateColor;
        private Integer vehicleType;
        private String specialFlag;
    }

    /**
     * RealTimeSeg是固定时间间隔每公里某方向的路况统计类
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    @Setter
    public static class RealTimeSeg implements Serializable {
        // 公里段范围
        String stakeSegId;

        // 方向
        int direction;

        //平均速度
        private double averageSpeed;

        //车密度  车数/公里数
        private int vehicleDensity;

        // 车道密度  车数/公里数
        private int laneNo1Density;
        private int laneNo2Density;
        private int laneNo3Density;
        private int laneNo4Density;

        // 拥塞指数
        private double congestionIndex;

        // 客货车数量
        private int busCount;
        private int truckCount;

        // 客货车平均速度
        private double busAverageSpeed;
        private double trackAverageSpeed;
    }

    /**
     * RealTimeSpatialData是固定时间间隔整条路（/大路段）的每公里的路况汇总类
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    @Setter
    public static class RealTimeSpatialData implements Serializable {
        // 事件时间
        String timestamp;

        // 时间时间 - millis
        long time;

        // 推送时间 - 本机事件 -> 但集群本机时间错误，这个先不填
        String uploadTime = null;

        // 包含的每公里的路况
        List<RealTimeSeg> realTimeSegList;
    }


    /**
     * MileageSegAssignment 会在主程序的static代码块中加载已知的gantry信息
     */
    @Getter
    @Setter
    public static class MileageSegAssignment implements Serializable {
        private List<Pair<Double, Double>> mileageSegments;

        public MileageSegAssignment(String excelFilePath) throws IOException {
            // 从Excel文件加载卡口信息
            this.mileageSegments = loadCheckpointsFromExcel(excelFilePath);
        }

        private List<Pair<Double, Double>> loadCheckpointsFromExcel(String filePath) throws IOException {
            List<Pair<Double, Double>> mileageSegments = new ArrayList<>();
            try (FileInputStream fis = new FileInputStream(filePath);
            Workbook workbook = new XSSFWorkbook(fis);) {
                Sheet sheet = workbook.getSheetAt(0);

                for (int rowNum = 1; rowNum <= sheet.getLastRowNum(); rowNum++) {
                    Row row = sheet.getRow(rowNum);
                    if (row != null) {
                        double longitude = row.getCell(1).getNumericCellValue();
                        double latitude = row.getCell(2).getNumericCellValue();
                        mileageSegments.add(Pair.of(longitude, latitude));
                    }
                }
            }

            // 按纬度由大到小排序
            mileageSegments.sort((o1, o2) -> {
                // 降序排序，如果o2的纬度大于o1的纬度，则返回负数
                return Double.compare(o2.getRight(), o1.getRight());
            });

            return mileageSegments;
        }

        public int findInsertionIndex(double targetLatitude) {
            int left = 0;
            int right = mileageSegments.size() - 1;

            while (left <= right) {
                int mid = left + (right - left) / 2;
                double midLatitude = mileageSegments.get(mid).getValue();

                if (midLatitude == targetLatitude) {
                    return mid + 1;
                } else if (midLatitude < targetLatitude) {
                    right = mid - 1;
                } else {
                    left = mid + 1;
                }
            }

            return left;
        }
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
    public static class StakeAssignment implements Serializable {
        private static final double EARTH_RADIUS = 6371.393;
        private List<StakeInfo> stakeInfoList;

        public StakeAssignment(String resourceName) throws IOException {
            try (InputStream is = getClass().getClassLoader().getResourceAsStream(resourceName)) {
                if (is == null) {
                    throw new FileNotFoundException("资源未找到: " + resourceName);
                }
                this.stakeInfoList = loadCheckpointsFromJSON(is);
            }
        }
        private List<StakeInfo> loadCheckpointsFromJSON(InputStream is) throws IOException {
            // 替换 readAllBytes 为兼容 Java 8 的读取方式
            ByteArrayOutputStream result = new ByteArrayOutputStream();
            byte[] buffer = new byte[1024];
            int length;
            while ((length = is.read(buffer)) != -1) {
                result.write(buffer, 0, length);
            }
            String json = new String(result.toByteArray(), StandardCharsets.UTF_8);

            List<StakeInfo> list = JSON.parseObject(json, new TypeReference<List<StakeInfo>>() {});


            return list;
        }

        public String findInsertionIndex(double lng, double lat) {
            int left = 0;
            int right = stakeInfoList.size() - 1;

            while (left <= right) {
                int mid = left + (right - left) / 2;
                double[] coordinate = stakeInfoList.get(mid).getLnglat();
                double midDistance = calculateDistance(coordinate[0], coordinate[1], lng, lat) * 1000;

                if (midDistance <= 0.5) {
                    return stakeInfoList.get(mid).getStake();
                } else if (coordinate[1] < lat) {
                    right = mid - 1;
                } else {
                    left = mid + 1;
                }
            }

            return stakeInfoList.get(left).getStake();
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

        private String readFileContent(String filePath) {
            try {
                // 使用 Files.readAllBytes 方法读取文件内容
                byte[] bytes = Files.readAllBytes(Paths.get(filePath));
                return new String(bytes);
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        }

        private int stakeToMileage(String stakeId) {
            return Integer.parseInt(stakeId.split("\\+")[0].substring(1)) * 1000 + Integer.parseInt(stakeId.split("\\+")[1]);
        }
    }

    /**
     * StakeAssignment 会在主程序的static代码块中加载已知的gantry信息
     */
    @Getter
    @Setter
    public static class MileageConverter implements Serializable {
        private Map<Integer, StakeInfo> stakeInfoMap;
        private List<Integer> mileageList;

        public MileageConverter(String resourceName) throws IOException {
            try (InputStream is = getClass().getClassLoader().getResourceAsStream(resourceName)) {
                if (is == null) {
                    throw new FileNotFoundException("资源未找到: " + resourceName);
                }
                this.stakeInfoMap = loadCheckpointsFromJSON(is);
                this.mileageList = new ArrayList<>(stakeInfoMap.keySet());
                this.mileageList.sort(Integer::compare);
            }
        }

        private Map<Integer, StakeInfo> loadCheckpointsFromJSON(InputStream is) throws IOException {
            ByteArrayOutputStream result = new ByteArrayOutputStream();
            byte[] buffer = new byte[1024];
            int length;
            while ((length = is.read(buffer)) != -1) {
                result.write(buffer, 0, length);
            }
            String json = new String(result.toByteArray(), StandardCharsets.UTF_8);
            List<StakeInfo> list = JSON.parseObject(json, new TypeReference<List<StakeInfo>>() {});

            Map<Integer, StakeInfo> map = new HashMap<>();
            for (StakeInfo info : list) {
                try {
                    int mileage = stakeToMileage(info.getStake());
                    map.put(mileage, info);
                } catch (Exception e) {
                    System.err.println("桩号解析失败: " + info.getStake());
                }
            }
            return map;
        }

        private List<Integer> loadSortedList(Map<Integer, StakeInfo> stakeInfoMap) {
            List<Integer> mileageList = new ArrayList<>();
            mileageList.addAll(stakeInfoMap.keySet());
            mileageList.sort(Integer::compare);
            return mileageList;
        }

        public StakeInfo findCoordinate(int targetMileage) {
            if(targetMileage < mileageList.get(0))
                return stakeInfoMap.get(mileageList.get(0));
            else if(targetMileage > mileageList.get(mileageList.size() - 1))
                return stakeInfoMap.get(mileageList.get(mileageList.size() - 1));
            return stakeInfoMap.get(targetMileage);
        }

        private String readFileContent(String filePath) {
            try {
                // 使用 Files.readAllBytes 方法读取文件内容
                byte[] bytes = Files.readAllBytes(Paths.get(filePath));
                return new String(bytes);
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        }

        private int stakeToMileage(String stakeId) {
            return Integer.parseInt(stakeId.split("\\+")[0].substring(1)) * 1000 + Integer.parseInt(stakeId.split("\\+")[1]);
        }

    }

}
