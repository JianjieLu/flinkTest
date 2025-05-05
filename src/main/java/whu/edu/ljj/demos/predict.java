//package whu.edu.ljj.demos;
//
//import org.apache.flink.api.common.state.ListState;
//import org.zeromq.ZContext;
//import org.zeromq.ZMQ;
//import org.zeromq.ZMsg;
//
//import java.io.FileWriter;
//import java.io.IOException;
//import java.io.PrintWriter;
//import java.nio.charset.StandardCharsets;
//import java.util.Date;
//import java.io.*;
//import java.text.SimpleDateFormat;
//import java.util.*;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.regex.Pattern;
//
//public class predict {
//    private static final int WINDOW_SIZE = 10;//用来预测的窗口大小
//    private static final LinkedList<Double> speedWindow = new LinkedList<>();
//    private static final Map<Long, VehicleData> vehicleMap = new ConcurrentHashMap<>();
//
//    static int timeout=300;
//    private static final int TIMEOUT_MS = 1000; // 超时时间
//    public static void main(String[] args) throws IOException {
//        // 检查命令行参数
////        String publisherAddress = "tcp://*:5563";  // 测试发布者地址
//        String publisherAddress = "tcp://100.65.62.82:8030";  // 发布者地址
//        String outputFile = "D:\\轨迹存储论文\\123.txt";  // 输出文件名
//        int ssn=0;
//        ListState<String> dal;
//
//    boolean firstEnter=true;
//        // 创建ZMQ上下文和套接字
//        try (ZContext context = new ZContext();
//             FileWriter fileWriter = new FileWriter(outputFile);
//             PrintWriter printWriter = new PrintWriter(fileWriter)) {
//
//            // 创建订阅者套接字
//            ZMQ.Socket subscriber = context.createSocket(ZMQ.SUB);
//            subscriber.connect(publisherAddress);  // 连接到发布者
//            subscriber.subscribe(ZMQ.SUBSCRIPTION_ALL);
//            subscriber.setReceiveTimeOut(timeout);  // 超时时间
//
//            while (!Thread.currentThread().isInterrupted()) {
//                // 接收消息
//                ZMsg msg = ZMsg.recvMsg(subscriber);
//                if(msg!=null){
//                String line = msg.toString();
//                line = line.replaceAll("[\\[\\]]", "").trim();
//                byte[] data = hexStringToByteArray(line);
//                int sn = byteArrayToIntLittleEndian(data, 0);
//                String deviceIp = byteArrayToIp(data, 4);
//                long time = byteArrayToLongLittleEndian(data, 8);
//                int count = byteArrayToIntLittleEndian(data, 16);
//                List<vehicleInfo> vehicles = new ArrayList<>();
//                for (int i = 0; i < count; i++) {
//                    int offset = 20 + i * 44; // 每个TDATA占44字节
//                    long carId = byteArrayToLongLittleEndian(data, offset);
//                    String carNumber = new String(data, offset + 8, 16, StandardCharsets.UTF_8).trim();
//                    byte type = data[offset + 24];
//
//                    List<Integer> scope = new ArrayList<>();
//                    scope.add(byteArrayToIntLittleEndian(data, offset + 25));
//                    scope.add(byteArrayToIntLittleEndian(data, offset + 29));
//                    float speed = Float.intBitsToFloat(byteArrayToIntLittleEndian(data, offset + 33));
//                    byte wayno = data[offset + 37];
//                    int tpointno = byteArrayToIntLittleEndian(data, offset + 38);
//                    //byte booleanValue = data[offset + 42];
//                    byte direct = data[offset + 43];
//                    vehicles.add(new vehicleInfo(carId, speed, wayno, tpointno, type, direct, scope));
//
//                }
//                DataPrinter printer = new DataPrinter();
//                sensorData s = new sensorData(sn, deviceIp, time, count, vehicles);
//                if (s != null) {
//                    printer.printOneJson(s);
//
//                    updateVehicleData(s); // 更新车辆数据
//                    if (firstEnter) {
//                        ssn = s.sn;
//                        firstEnter = false;
//                    } else {
//                        ssn = handleSequence(s.sn, ssn);
//                    }
//                }
//                msg.destroy();
//            }else{
//                handleTimeoutPrediction(printWriter); // 触发超时预测
//            }
//
//}
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//    private static void handleTimeoutPrediction(PrintWriter writer) {
//        long currentTime = System.currentTimeMillis();
//        for (Map.Entry<Long, Utils.VehicleData> entry : vehicleMap.entrySet()) {
//            long carId = entry.getKey();
//              Utils.VehicleData data = entry.getValue();
//            synchronized (data) {
//                if (currentTime - data.lastReceivedTime >= TIMEOUT_MS && !data.speedWindow.isEmpty()) {
//                    // 使用车辆独立窗口计算
//                    double predictedSpeed = calculateMovingAverage(data.speedWindow);
//                    data.speedWindow.addLast(predictedSpeed);
//                    data.speedWindow.removeFirst();
//                    int distanceDiff = (int) (predictedSpeed * 0.2 ); // 米
//                    int newTpointno = data.tpointno + distanceDiff; // 更新里程点
//                    data.tpointno=newTpointno;
////                long carid=data.carid;
//                    int carType=data.cartype;
//                    // 输出预测结果
//                    String output = String.format(
//                            "[预测]time:%tT Car ID:%d  Car Number:    Type:%d  Scope:[0,0]  Speed:%.14f m/s  Wayno:%d  Tpointno:%d  Boolean:0  Direct:%d (基于最近%d条数据)",
//                            new Date(currentTime),carId, carType,predictedSpeed,data.wayno, newTpointno, data.direct,data.speedWindow.size()
//                    );
//                    System.out.println(output);
//                    writer.println(output);
//                }
//            }
//        }
//    }
//    private static double calculateMovingAverage(LinkedList<Double> speedWindow) {
//        synchronized (speedWindow) {
//            return speedWindow.stream()
//                    .mapToDouble(Double::doubleValue)
//                    .average()
//                    .orElse(Double.NaN);
//        }
//    }
//    private static int handleSequence(int currentSN, int lastSN) {
//        if (currentSN != lastSN + 1) {
//            System.out.printf("丢包! 当前SN: %d, 期望SN: %d%n", currentSN, lastSN + 1);
//        }
//        return currentSN;
//    }
//    private static void updateVehicleData( sensorData sensorData) {
//        long currentTime = System.currentTimeMillis();
//        for ( vehicleInfo vehicle : sensorData.vehicles) {//遍历当前这条sensordata消息中的所有vehicleinfo
//             VehicleData data = vehicleMap.computeIfAbsent(vehicle.carId, k -> new  VehicleData());
//            synchronized (data) {
//                // 更新车辆独立窗口（原data.speedWindow）
//                data.speedWindow.add(vehicle.speed);
//                if (data.speedWindow.size() > WINDOW_SIZE) {
//                    data.speedWindow.removeFirst();
//                }
//                data.tpointno=vehicle.tpointno;
//            }
//        }
//    }
//    private static byte[] hexStringToByteArray(String s) {
//        int len = s.length();
//        byte[] data = new byte[len / 2];
//        for (int i = 0; i < len; i += 2) {
//            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
//                    + Character.digit(s.charAt(i+1), 16));
//        }
//        return data;
//    }
//
//    private static String byteArrayToIp(byte[] bytes, int offset) {
//        return ((bytes[offset] & 0xFF) + "." +
//                (bytes[offset + 1] & 0xFF) + "." +
//                (bytes[offset + 2] & 0xFF) + "." +
//                (bytes[offset + 3] & 0xFF));
//    }
//
//    private static int byteArrayToIntLittleEndian(byte[] bytes, int offset) {
//        return ((bytes[offset] & 0xFF) |
//                ((bytes[offset + 1] & 0xFF) << 8) |
//                ((bytes[offset + 2] & 0xFF) << 16) |
//                ((bytes[offset + 3] & 0xFF) << 24));
//    }
//
//    private static long byteArrayToLongLittleEndian(byte[] bytes, int offset) {
//        return ((long) bytes[offset] & 0xFF) |
//                ((long) bytes[offset + 1] & 0xFF) << 8 |
//                ((long) bytes[offset + 2] & 0xFF) << 16 |
//                ((long) bytes[offset + 3] & 0xFF) << 24 |
//                ((long) bytes[offset + 4] & 0xFF) << 32 |
//                ((long) bytes[offset + 5] & 0xFF) << 40 |
//                ((long) bytes[offset + 6] & 0xFF) << 48 |
//                ((long) bytes[offset + 7] & 0xFF) << 56;
//    }
//}
////class VehicleData{
////    long lastReceivedTime;
////    LinkedList<Double> speedWindow = new LinkedList<>();
////    int wayno;
////    int tpointno;
////    long carid;
////    int cartype;
////    int direct;
////}
//class DataParser {
//    private static final Pattern INDEX_PATTERN = Pattern.compile("^\\d+\\.\\s*");
//
//    public List<sensorData> parseFile(String filePath) {
//        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
//            StringBuilder content = new StringBuilder();
//            String line;
//            while ((line = reader.readLine()) != null) {
//                content.append(line).append("\n");
//            }
//            return parse(content.toString());
//        } catch (IOException e) {
//            System.err.println("文件解析错误: " + e.getMessage());
//            return Collections.emptyList();
//        }
//    }
//
//    private List<sensorData> parse(String content) {
//        List<sensorData> entries = new ArrayList<>();
//        Map<String, Object> currentEntry = null;
//        Map<String, Object> currentVehicle = null;
//
//        int lineNumber = 0;
//        for (String rawLine : content.split("\n")) {
//            lineNumber++;
//            String line = rawLine.trim();
//            if (line.isEmpty()) continue;
//
//            try {
//                if (line.startsWith("SN:")) {
//                    if (currentEntry != null) {
//                        entries.add(finalizeEntry(currentEntry));
//                    }
//                    currentEntry = initNewEntry(line);
//                    continue;
//                }
//
//                if (line.startsWith("====")) {
//                    if (currentVehicle != null && currentEntry != null) {
//                        currentEntry = addVehicle(currentEntry, currentVehicle);
//                        currentVehicle = null;
//                    }
//                    continue;
//                }
//
//                String[] parts = parseLine(line);
//                if (parts == null) continue;
//
//                String key = parts[0];
//                String value = parts[1];
//
//                if (Arrays.asList("device_ip", "time", "count").contains(key)) {
//                    if (currentEntry == null) {
//                        System.err.printf("第%d行错误：传感器数据出现在SN声明之前%n", lineNumber);
//                        continue;
//                    }
//                    updateSensorData(currentEntry, key, value);
//                } else if (Arrays.asList("car_id", "type", "scope", "speed",
//                        "wayno", "tpointno", "direct").contains(key)) {
//                    if (currentEntry == null) {
//                        System.err.printf("第%d行错误：车辆数据出现在SN声明之前%n", lineNumber);
//                        continue;
//                    }
//                    currentVehicle = updateVehicleData(currentVehicle, key, value);
//                }
//            } catch (Exception e) {
//                System.err.printf("第%d行解析错误: %s%n", lineNumber, e.getMessage());
//            }
//        }
//
//        if (currentEntry != null) {
//            entries.add(finalizeEntry(currentEntry));
//        }
//        return entries;
//    }
//
//    private Map<String, Object> initNewEntry(String line) {
//        String[] parts = line.split(":", 2);
//        if (parts.length < 2) {
//            System.err.println("SN格式错误: " + line);
//            return null;
//        }
//        return new HashMap<String, Object>() {{
//            put("sn", Integer.parseInt(parts[1].trim()));
//            put("device_ip", "");
//            put("timestamp", 0L);
//            put("count", 0);
//            put("vehicles", new ArrayList<vehicleInfo>());
//        }};
//    }
//
//    private String[] parseLine(String line) {
//        try {
//            String cleanLine = INDEX_PATTERN.matcher(line).replaceFirst("");
//            String[] parts = cleanLine.split(":\\s*", 2);
//            return new String[]{
//                    parts[0].trim().toLowerCase().replace(' ', '_'),
//                    parts.length > 1 ? parts[1].trim() : ""
//            };
//        } catch (Exception e) {
//            return null;
//        }
//    }
//
//    private void updateSensorData(Map<String, Object> entry, String key, String value) {
//        switch (key) {
//            case "device_ip":
//                entry.put("device_ip", value);
//                break;
//            case "time":
//                entry.put("timestamp", Long.parseLong(value));
//                break;
//            case "count":
//                entry.put("count", Integer.parseInt(value));
//                break;
//        }
//    }
//
//    private Map<String, Object> updateVehicleData(Map<String, Object> vehicle,
//                                                  String key, String value) {
//        if (vehicle == null) vehicle = new HashMap<>();
//        try {
//            switch (key) {
//                case "car_id":
//                    vehicle.put("car_id", Integer.parseInt(value));
//                    break;
//                case "type":
//                    vehicle.put("car_type", Integer.parseInt(value));
//                    break;
//                case "scope":
//                    vehicle.put("scope", parseScope(value));
//                    break;
//                case "speed":
//                    vehicle.put("speed", Double.parseDouble(value.split(" ")[0]));
//                    break;
//                case "wayno":
//                    vehicle.put("wayno", Integer.parseInt(value));
//                    break;
//                case "tpointno":
//                    vehicle.put("tpointno", parseTpointno(value));
//                    break;
//                case "direct":
//                    vehicle.put("direct", Integer.parseInt(value));
//                    break;
//            }
//        } catch (Exception e) {
//            System.err.println("数据转换错误: " + e.getMessage());
//        }
//        return vehicle;
//    }
//
//    private List<Integer> parseScope(String value) {
//        String clean = value.replaceAll("[^0-9,-]", "");
//        List<Integer> result = new ArrayList<>();
//        for (String num : clean.split(",")) {
//            try {
//                result.add(Integer.parseInt(num.trim()));
//            } catch (NumberFormatException e) {
//                System.err.println("无效的Scope数值: '" + num + "'");
//            }
//        }
//        return result;
//    }
//
//    private int parseTpointno(String value) {
//        try {
//            return Integer.parseInt(value.replaceAll("[^0-9]", ""));
//        } catch (NumberFormatException e) {
//            System.err.println("里程解析失败: " + value);
//            return 0;
//        }
//    }
//
//    @SuppressWarnings("unchecked")
//    private Map<String, Object> addVehicle(Map<String, Object> entry,
//                                           Map<String, Object> vehicle) {
//        Set<String> required = new HashSet<>(Arrays.asList(
//                "car_id", "car_type", "scope", "speed", "wayno", "tpointno", "direct"));
//        if (!vehicle.keySet().containsAll(required)) {
//            System.err.println("丢弃不完整车辆数据");
//            return entry;
//        }
//
//        List<vehicleInfo> vehicles = (List<vehicleInfo>) entry.get("vehicles");
//        vehicles.add(new vehicleInfo(
//                (Integer) vehicle.get("car_id"),
//                (Double) vehicle.get("speed"),
//                (Integer) vehicle.get("wayno"),
//                (Integer) vehicle.get("tpointno"),
//                (Integer) vehicle.get("car_type"),
//                (Integer) vehicle.get("direct"),
//                (List<Integer>) vehicle.get("scope")
//        ));
//
//        return entry;
//    }
//
//    @SuppressWarnings("unchecked")
//    private sensorData finalizeEntry(Map<String, Object> entry) {
//        int declaredCount = (Integer) entry.get("count");
//        List<vehicleInfo> vehicles = (List<vehicleInfo>) entry.get("vehicles");
//        if (vehicles.size() != declaredCount) {
//            System.err.printf("SN %d 数量不一致: 声明%d 实际%d%n",
//                    entry.get("sn"), declaredCount, vehicles.size());
//        }
//
//        return new sensorData(
//                (Integer) entry.get("sn"),
//                (String) entry.get("device_ip"),
//                (Long) entry.get("timestamp"),
//                (Integer) entry.get("count"),
//                vehicles
//        );
//    }
//}
//
//// 数据打印机
//class DataPrinter {
//    public void printAll(List<sensorData> data, String mode, boolean detailed) {
//        if (data.isEmpty()) {
//            System.out.println("没有可输出的数据");
//            return;
//        }
//
//        System.out.println("\n================== 详细数据报告 ==================\n");
//
//        if ("table".equals(mode)) {
//            printTable(data, detailed);
//        } else if ("json".equals(mode)) {
//            printJson(data);
//        } else {
//            printPlain(data, detailed);
//        }
//
//        System.out.println("\n================== 数据结束 ==================\n");
//    }
//
//    private void printTable(List<sensorData> data, boolean detailed) {
//        if (!detailed) {
//            System.out.println("SN\t时间\t设备IP\t车辆数");
//            for (sensorData entry : data) {
//                System.out.printf("%d\t%s\t%s\t%d/%d%n",
//                        entry.sn,
//                        formatTimestamp(entry.timestamp),
//                        entry.deviceIp,
//                        entry.vehicles.size(),
//                        entry.count);
//            }
//            return;
//        }
//
//        for (sensorData entry : data) {
//            System.out.println("========================================");
//            System.out.printf("SN: %d\n时间: %s\n设备IP: %s\n车辆数: %d/%d\n",
//                    entry.sn,
//                    formatTimestamp(entry.timestamp),
//                    entry.deviceIp,
//                    entry.vehicles.size(),
//                    entry.count);
//
//            System.out.println("+--------+------+---------+------+-------+------+");
//            System.out.println("| 车辆ID | 类型 | 速度(m/s)| 车道 | 里程  | 方向 |");
//            System.out.println("+--------+------+---------+------+-------+------+");
//            for (vehicleInfo v : entry.vehicles) {
//                System.out.printf("| %6d | %4d | %7.2f | %4d | %5d | %4d |%n",
//                        v.carId, v.carType, v.speed,
//                        v.wayno, v.tpointno, v.direction);
//            }
//            System.out.println("+--------+------+---------+------+-------+------+");
//        }
//    }
//
//    private String formatTimestamp(long timestamp) {
//        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//                .format(new Date(timestamp));
//    }
//
//    private void printJson(List<sensorData> data) {
//        StringBuilder json = new StringBuilder("[\n");
//        for (sensorData entry : data) {
//            json.append("  {\n")
//                    .append("    \"sn\": ").append(entry.sn).append(",\n")
//                    .append("    \"timestamp\": ").append(entry.timestamp).append(",\n")
//                    .append("    \"deviceIp\": \"").append(entry.deviceIp).append("\",\n")
//                    .append("    \"count\": ").append(entry.count).append(",\n")
//                    .append("    \"vehicles\": [\n");
//
//            for (vehicleInfo v : entry.vehicles) {
//                json.append("      {\n")
//                        .append("        \"carId\": ").append(v.carId).append(",\n")
//                        .append("        \"speed\": ").append(v.speed).append("\n")
//                        .append("      },\n");
//            }
//            if (!entry.vehicles.isEmpty()) json.setLength(json.length()-2);
//            json.append("\n    ]\n  },\n");
//        }
//        if (!data.isEmpty()) json.setLength(json.length()-2);
//        json.append("\n]");
//        System.out.println(json);
//    }
//
//    void printOneJson(sensorData entry) {
//        StringBuilder json = new StringBuilder();
//
//            json.append("  {\n")
//                    .append("    \"sn\": ").append(entry.sn).append(",\n")
//                    .append("    \"timestamp\": ").append(entry.timestamp).append(",\n")
//                    .append("    \"deviceIp\": \"").append(entry.deviceIp).append("\",\n")
//                    .append("    \"count\": ").append(entry.count).append(",\n")
//                    .append("    \"vehicles\": [\n");
//            for (vehicleInfo v : entry.vehicles) {
//                json.append("      {\n")
//                        .append("        \"carId\": ").append(v.carId).append(",\n")
//                        .append("        \"speed\": ").append(v.speed).append("\n")
//                        .append("        \"wayno\": ").append(v.wayno).append("\n")
//                        .append("        \"tpointno\": ").append(v.tpointno).append("\n")
//                        .append("        \"carType\": ").append(v.carType).append("\n")
//                        .append("        \"direction\": ").append(v.direction).append("\n")
//                        .append("        \"scope\": ").append(v.scope).append("\n")
//                        .append("      },\n");
//            }
//            if (!entry.vehicles.isEmpty()) json.setLength(json.length()-2);
//            json.append("\n    ]\n  },\n");
//        System.out.println(json);
//    }
//    private void printPlain(List<sensorData> data, boolean detailed) {
//        for (int i = 0; i < data.size(); i++) {
//            sensorData entry = data.get(i);
//            System.out.printf("\n条目 #%d\n", i+1);
//            System.out.printf("  SN: %d\n  时间: %s\n  设备: %s\n  车辆: %d/%d\n",
//                    entry.sn, formatTimestamp(entry.timestamp),
//                    entry.deviceIp, entry.vehicles.size(), entry.count);
//            if (detailed) {
//                for (vehicleInfo v : entry.vehicles) {
//                    System.out.printf("  - ID: %d, 速度: %.2f m/s, 位置: Way%d-%d, 方向: %d%n",
//                            v.carId, v.speed, v.wayno, v.tpointno, v.direction);
//                }
//            }
//        }
//    }
//}
//class vehicleInfo {
//    public long carId;
//    public double speed;
//    public int wayno;
//    public int tpointno;
//    public int carType;
//    public int direction;
//    public List<Integer> scope;
//
//    public vehicleInfo(long carId, double speed, int wayno, int tpointno,int carType,int direction,List<Integer> scope) {
//        this.carId = carId;
//        this.speed = speed;
//        this.wayno = wayno;
//        this.tpointno = tpointno;
//        this.carType = carType;
//        this.direction = direction;
//        this.scope=scope;
//    }
//
//    public vehicleInfo() {
//
//    }
//}
//class sensorData {
//    public int sn;
//    public String deviceIp;
//    public long timestamp;
//    public int count;
//    public List<vehicleInfo> vehicles;
//
//    public sensorData(int sn, String deviceIp, long timestamp,
//                      int count, List<vehicleInfo> vehicles) {
//        this.sn = sn;
//        this.deviceIp = deviceIp;
//        this.timestamp = timestamp;
//        this.count = count;
//        this.vehicles = vehicles;
//    }
//
//    public sensorData() {
//
//    }
//}