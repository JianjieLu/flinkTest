package whu.edu.ljj.flink.utils;

import lombok.Getter;
import lombok.Setter;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;
import java.util.Objects;
//import static whu.edu.ljj.flink.xiaohanying.Utils.*;
import static whu.edu.ljj.flink.xiaohanying.Utils.*;

public class myTools {

    public static String convert(long timestamp, String pattern, String zoneId) {
        Instant instant = Instant.ofEpochMilli(timestamp);
        ZonedDateTime zdt = ZonedDateTime.ofInstant(instant, ZoneId.of(zoneId));
        return DateTimeFormatter.ofPattern(pattern).format(zdt);
    }
    // 快捷方法
    public static String toDateTimeString(long timestamp) {

        return convert(timestamp, "yyyy-MM-dd HH:mm:ss:SSS", "Asia/Shanghai");
    }

    public static Long toDateTimeLong(String timeStr){
        // 创建格式化器
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS");
        // 解析为LocalDateTime
        LocalDateTime localDateTime = LocalDateTime.parse(timeStr, formatter);
        // 应用上海时区
        ZonedDateTime shanghaiTime = localDateTime.atZone(ZoneId.of("Asia/Shanghai"));
        // 转换为时间戳（毫秒）
        return shanghaiTime.toInstant().toEpochMilli();
    }
    public static void printOneMergeData(PathTData PathTData){
        // 结构化输出主信息
        System.out.println("\n=== 接收到MergeData ===");
        System.out.printf("时间戳: %s | 路段ID: %s | 车辆数: %d%n",
                PathTData.getTimeStamp(),
                PathTData.getWaySectionId(),
                PathTData.getPathNum());

        // 遍历输出路径详情
        PathTData.getPathList().forEach(point -> {
            System.out.printf("├─ 车辆轨迹点  carid:%d | 里程:%d | 车牌: %s (颜色:%d) | 坐标: %.6f,%.6f | 速度: %.2f km/h | 车道: %d | 方向: %d%n",
                    point.getId(),
                    point.getMileage(),
                    point.getPlateNo(),
                    point.getPlateColor(),
                    point.getLongitude(),
                    point.getLatitude(),
                    point.getSpeed(),
                    point.getLaneNo(),
                    point.getDirection());

        });
    }
    public static void printOneMergeDataWithoutPathList(PathTData PathTData){
        // 结构化输出主信息
        System.out.println("\n=== 接收到MergeData ===");
        System.out.printf("时间戳: %s | 路段ID: %s | 车辆数: %d%n",
                PathTData.getTimeStamp(),
                PathTData.getWaySectionId(),
                PathTData.getPathNum());
    }
    public static void getCaridXXXFromMergeData(PathTData mergeData,long carid){
            for(PathPoint m : mergeData.getPathList()){
                if(m.getId()==carid) printmergePoint(m);
            }
    }
    public static void printmergePoint(PathPoint point){
            System.out.printf("├─ 车辆轨迹点  carid:%d | 里程:%d | 时间：%s | 车牌: %s (颜色:%d) | 坐标: %.6f,%.6f | 速度: %.2f km/h | 车道: %d | 方向: %d%n",
                    point.getId(),
                    point.getMileage(),
                    point.getTimeStamp(),

                    point.getPlateNo(),
                    point.getPlateColor(),
                    point.getLongitude(),
                    point.getLatitude(),
                    point.getSpeed(),
                    point.getLaneNo(),
                    point.getDirection());

    }
    public static String PointToString(PathPoint point){
        return "├─ 车辆轨迹点  carid:"+point.getId()+" | 里程:"+point.getMileage()+" | 时间："+point.getTimeStamp()+" | 车牌: "+point.getPlateNo()+" (颜色:"+point.getPlateColor()+") | 坐标: "+point.getLongitude()+","+point.getLatitude()+" | 速度: "+point.getSpeed()+" km/h | 车道: "+point.getLaneNo()+" | 方向: "+point.getDirection()+" | 桩号: "+point.getStakeId()+" | 航向角: "+point.getCarAngle();
    }
    public static String PointToStringXiaoHanYing(PathPoint point){
        return "├─ 车辆轨迹点  carid:"+point.getId()+" | 里程:"+point.getMileage()+" | 时间："+point.getTimeStamp()+" | 车牌: "+point.getPlateNo()+" (颜色:"+point.getPlateColor()+") | 坐标: "+point.getLongitude()+","+point.getLatitude()+" | 速度: "+point.getSpeed()+" km/h | 车道: "+point.getLaneNo()+" | 方向: "+point.getDirection()+" | 桩号: "+point.getStakeId()+" | 航向角: "+point.getCarAngle();
    }
    public static String PointDataToString(PathPointData point){
        return "├─ 车辆轨迹点  carid:"+point.getId()+" | 里程:"+point.getMileage()+" | 时间："+point.getTimeStamp()+" | 车牌: "+point.getPlateNo()+" (颜色:"+point.getPlateColor()+") | 坐标: "+point.getLongitude()+","+point.getLatitude()+" | 速度: "+point.getSpeed()+" km/h | 车道: "+point.getLaneNo()+" | 方向: "+point.getDirection()+" | 桩号: "+point.getStakeId()+" | 航向角: "+point.getCarAngle();
    }
    public static void printmergePointData(PathPointData point){
        System.out.printf("├─ 车辆轨迹点  carid:%d | 里程:%d | 时间：%s | 车牌: %s (颜色:%d) | 坐标: %.6f,%.6f | 速度: %.2f km/h | 车道: %d | 方向: %d | 桩号: %s%n",
                point.getId(), point.getMileage(), point.getTimeStamp(), point.getPlateNo(), point.getPlateColor(), point.getLongitude(), point.getLatitude(), point.getSpeed(), point.getLaneNo(), point.getDirection(),point.getStakeId());
//        System.out.println(point);
    }
    public static void printPathPointData(PathPointData point){
        System.out.printf("├─ 车辆轨迹点  carid:%s | 车牌: %s | 里程:%d | 速度: %.2f km/h  | 时间：%s | 车道: %d | 方向: %d%n",
                point.getId(),
                point.getPlateNo(),
                point.getMileage(),
                point.getSpeed(),
                point.getTimeStamp(),
                point.getLaneNo(),
                point.getDirection());

    }
    public static void printPathPoint(PathPoint point){
        System.out.printf("├─ 车辆轨迹点  carid:%s | 车牌: %s | 里程:%d | 速度: %.2f km/h  | 时间：%s | 车道: %d | 方向: %d%n",
                point.getId(),
                point.getPlateNo(),
                point.getMileage(),
                point.getSpeed(),
                point.getTimeStamp(),
                point.getLaneNo(),
                point.getDirection());

    }


    public static double calculateBearing(double startLat, double startLon, double endLat, double endLon) {
        // 将十进制度数转换为弧度
        double lat1 = Math.toRadians(startLat);
        double lon1 = Math.toRadians(startLon);
        double lat2 = Math.toRadians(endLat);
        double lon2 = Math.toRadians(endLon);

        // 计算经度差
        double dLon = lon2 - lon1;

        // 计算方位角
        double y = Math.sin(dLon) * Math.cos(lat2);
        double x = Math.cos(lat1) * Math.sin(lat2)
                - Math.sin(lat1) * Math.cos(lat2) * Math.cos(dLon);

        double bearing = Math.atan2(y, x);

        // 将弧度转换为度数（0-360范围）
        bearing = Math.toDegrees(bearing);
        bearing = (bearing + 360) % 360;

        return bearing;
    }
    public static String getNString(String str,int n,int m){
        // 安全截取前N个字符
        return str.substring(n, Math.min(str.length(), m));
    }
    public static double calculateDistance(float speedKmh, int timeMs) {
        // 参数校验
        if (speedKmh < 0 || timeMs < 0) {
            System.out.println("speedKmh: "+speedKmh+"  time: "+timeMs);
            throw new IllegalArgumentException("速度和时间的值不能为负数");
        }

        // 换算公式：距离 = (速度km/h × 时间ms) / 3600
        return (speedKmh * timeMs) / 3600.0;
    }

    public static void main(String[] args) {
        System.out.println(toDateTimeString(System.currentTimeMillis()));

    }
}


