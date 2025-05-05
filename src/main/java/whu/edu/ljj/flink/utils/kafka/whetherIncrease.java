package whu.edu.ljj.flink.utils.kafka;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.util.List;
import java.util.stream.Collectors;

public class whetherIncrease {

    public static void main(String[] args) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            List<LocationData> data = mapper.readValue(
                    new File("D:\\learn\\codes\\a_idea_codes\\flinkTest\\src\\main\\java\\whu\\edu\\ljj\\flink\\utils\\kafka\\sx.json"),
                    new TypeReference<List<LocationData>>() {}
            );

            // 修改点：使用 collect(Collectors.toList())
            List<Double> latitudes = data.stream()
                    .map(location -> location.lnglat[1])
                    .collect(Collectors.toList());

            Monotonicity result = checkMonotonicity(latitudes);
            System.out.println("纬度变化趋势: " + result);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 检查单调性方法
    private static Monotonicity checkMonotonicity(List<Double> list) {
        if (list.size() <= 1) return Monotonicity.NOT_MONOTONIC;

        boolean increasing = true;
        boolean decreasing = true;

        for (int i = 1; i < list.size(); i++) {
            double prev = list.get(i - 1);
            double curr = list.get(i);

            if (curr < prev) increasing = false;
            if (curr > prev) decreasing = false;

            if (!increasing && !decreasing) break;
        }

        if (increasing) return Monotonicity.INCREASING;
        if (decreasing) return Monotonicity.DECREASING;
        return Monotonicity.NOT_MONOTONIC;
    }

    // 数据结构定义
    static class LocationData {
        public String stake;
        public double[] lnglat;
    }

    // 单调性枚举
    enum Monotonicity {
        INCREASING,   // 单调递增
        DECREASING,   // 单调递减
        NOT_MONOTONIC // 非单调
    }
}