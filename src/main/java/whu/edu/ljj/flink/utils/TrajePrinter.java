package whu.edu.ljj.flink.utils;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import static whu.edu.ljj.flink.xiaohanying.Utils.*;

// 数据打印机
public class TrajePrinter {
    public void printAll(List<TrajeData> data, String mode, boolean detailed) {
        if (data.isEmpty()) {
            System.out.println("没有可输出的数据");
            return;
        }

        System.out.println("\n================== 详细数据报告 ==================\n");

        if ("table".equals(mode)) {
            printTable(data, detailed);
        } else if ("json".equals(mode)) {
            printJson(data);
        } else {
            printPlain(data, detailed);
        }

        System.out.println("\n================== 数据结束 ==================\n");
    }

    private void printTable(List<TrajeData> data, boolean detailed) {
        if (!detailed) {
            System.out.println("SN\t时间\t设备IP\t车辆数");
            for (TrajeData entry : data) {
                System.out.printf("%d\t%s\t%s\t%d/%d%n",
                        entry.getSN(),
                        formatTimestamp(entry.getTIME()),
                        entry.getDEVICEIP(),
                        entry.getTDATA().size(),
                        entry.getCOUNT());
            }
            return;
        }

        for (TrajeData entry : data) {
            System.out.println("========================================");
            System.out.printf("SN: %d\n时间: %s\n设备IP: %s\n车辆数: %d/%d\n",
                    entry.getSN(),
                    formatTimestamp(entry.getTIME()),
                    entry.getDEVICEIP(),
                    entry.getTDATA().size(),
                    entry.getCOUNT());

            System.out.println("+--------+------+---------+------+-------+------+");
            System.out.println("| 车辆ID | 类型 | 速度(m/s)| 车道 | 里程  | 方向 |");
            System.out.println("+--------+------+---------+------+-------+------+");
            for (TrajePoint v : entry.getTDATA()) {
                System.out.printf("| %6d | %4d | %7.2f | %4d | %5d | %4d |%n",
                        v.getID(), v.getType(), v.getSpeed(),
                        v.getWayno(), v.getTpointno(), v.getDirect());
            }
            System.out.println("+--------+------+---------+------+-------+------+");
        }
    }

    private String formatTimestamp(long timestamp) {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                .format(new Date(timestamp));
    }

    private void printJson(List<TrajeData> data) {
        StringBuilder json = new StringBuilder("[\n");
        for (TrajeData entry : data) {
            json.append("  {\n")
                    .append("    \"sn\": ").append(entry.getSN()).append(",\n")
                    .append("    \"timestamp\": ").append(entry.getTIME()).append(",\n")
                    .append("    \"deviceIp\": \"").append(entry.getDEVICEIP()).append("\",\n")
                    .append("    \"count\": ").append(entry.getCOUNT()).append(",\n")
                    .append("    \"vehicles\": [\n");

            for (TrajePoint v : entry.getTDATA()) {
                json.append("      {\n")
                        .append("        \"carId\": ").append(v.getID()).append(",\n")
                        .append("        \"speed\": ").append(v.getSpeed()).append("\n")
                        .append("      },\n");
            }
            if (!entry.getTDATA().isEmpty()) json.setLength(json.length()-2);
            json.append("\n    ]\n  },\n");
        }
        if (!data.isEmpty()) json.setLength(json.length()-2);
        json.append("\n]");
        System.out.println(json);
    }

    public void printOneJson(TrajeData entry) {
        StringBuilder json = new StringBuilder();

        json.append("  {\n")
                .append("    \"sn\": ").append(entry.getSN()).append(",\n")
                .append("    \"timestamp\": ").append(entry.getTIME()).append(",\n")
                .append("    \"deviceIp\": \"").append(entry.getDEVICEIP()).append("\",\n")
                .append("    \"count\": ").append(entry.getCOUNT()).append(",\n")
                .append("    \"vehicles\": [\n");
        for (TrajePoint v : entry.getTDATA()) {
            json.append("      {\n")
                    .append("        \"carId\": ").append(v.getID()).append(",\n")
                    .append("        \"speed\": ").append(v.getSpeed()).append("\n")
                    .append("        \"wayno\": ").append(v.getWayno()).append("\n")
                    .append("        \"tpointno\": ").append(v.getTpointno()).append("\n")
                    .append("        \"carType\": ").append(v.getType()).append("\n")
                    .append("        \"direction\": ").append(v.getDirect()).append("\n")
                    .append("        \"scope\": ").append(v.getScope()).append("\n")
                    .append("      },\n");
        }
        if (!entry.getTDATA().isEmpty()) json.setLength(json.length()-2);
        json.append("\n    ]\n  },\n");
        System.out.println(json);
    }
    private void printPlain(List<TrajeData> data, boolean detailed) {
        for (int i = 0; i < data.size(); i++) {
            TrajeData entry = data.get(i);
            System.out.printf("\n条目 #%d\n", i+1);
            System.out.printf("  SN: %d\n  时间: %s\n  设备: %s\n  车辆: %d/%d\n",
                    entry.getSN(), formatTimestamp(entry.getTIME()),
                    entry.getDEVICEIP(), entry.getTDATA().size(), entry.getCOUNT());
            if (detailed) {
                for (TrajePoint v : entry.getTDATA()) {
                    System.out.printf("  - ID: %d, 速度: %.2f m/s, 位置: Way%d-%d, 方向: %d%n",
                            v.getID(), v.getSpeed(), v.getWayno(), v.getTpointno(), v.getDirect());
                }
            }
        }
    }
}
