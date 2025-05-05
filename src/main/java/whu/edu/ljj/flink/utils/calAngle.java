package whu.edu.ljj.flink.utils;

import java.awt.geom.Point2D;

public class calAngle {
    public static double calculateBearing(double lat1, double lon1, double lat2, double lon2) {
        // 将经纬度转换为弧度
        double lat1Rad = Math.toRadians(lat1);
        double lon1Rad = Math.toRadians(lon1);
        double lat2Rad = Math.toRadians(lat2);
        double lon2Rad = Math.toRadians(lon2);

        // 计算经度差
        double deltaLon = lon2Rad - lon1Rad;

        // 计算方位角公式参数
        double y = Math.sin(deltaLon) * Math.cos(lat2Rad);
        double x = Math.cos(lat1Rad) * Math.sin(lat2Rad)
                - Math.sin(lat1Rad) * Math.cos(lat2Rad) * Math.cos(deltaLon);

        // 计算弧度角并转换为度数
        double bearingRad = Math.atan2(y, x);
        double bearingDeg = Math.toDegrees(bearingRad);

        // 转换为0-360度范围
        return (bearingDeg + 360) % 360;
    }
    // 计算两个线段之间的夹角（单位：度）
    public static double calculateAngleBetweenLines(LineSegment line1, LineSegment line2) {
        // 获取两个线段的方向向量
        Point2D.Double vec1 = getDirectionVector(line1);
        Point2D.Double vec2 = getDirectionVector(line2);

        // 计算点积
        double dotProduct = vec1.x * vec2.x + vec1.y * vec2.y;

        // 计算向量长度
        double len1 = Math.hypot(vec1.x, vec1.y);
        double len2 = Math.hypot(vec2.x, vec2.y);

        // 处理零向量情况
        if(len1 == 0 || len2 == 0) {
            throw new IllegalArgumentException("线段长度不能为零");
        }

        // 计算余弦值并限制范围（防止浮点误差）
        double cosTheta = dotProduct / (len1 * len2);
        cosTheta = Math.max(-1.0, Math.min(1.0, cosTheta));

        // 计算弧度并转为角度
        return Math.toDegrees(Math.acos(cosTheta));
    }

    // 获取线段方向向量（终点 - 起点）
    private static Point2D.Double getDirectionVector(LineSegment line) {
        return new Point2D.Double(
                line.end.x - line.start.x,
                line.end.y - line.start.y
        );
    }

    // 线段类定义
    public static class LineSegment {
        public final Point2D.Double start;
        public final Point2D.Double end;

        public LineSegment(Point2D.Double start, Point2D.Double end) {
            this.start = start;
            this.end = end;
        }
    }

    // 示例用法
    public static void main(String[] args) {
        // 创建两个线段
        LineSegment line1 = new LineSegment(
                new Point2D.Double(0, 0),
                new Point2D.Double(1, 1)  // 45度方向
        );

        LineSegment line2 = new LineSegment(
                new Point2D.Double(0, 0),
                new Point2D.Double(0, 1)  // 垂直向上
        );

        try {
            double angle = calculateAngleBetweenLines(line1, line2);
            System.out.printf("线段间夹角：%.2f 度", angle);
        } catch (IllegalArgumentException e) {
            System.out.println(e.getMessage());
        }
    }
}