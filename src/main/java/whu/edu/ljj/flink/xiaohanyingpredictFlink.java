//package whu.edu.ljj.flink;
//
//import com.alibaba.fastjson2.JSON;
//import com.alibaba.fastjson2.JSONObject;
//import org.apache.flink.api.common.functions.FlatMapFunction;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.util.Collector;
//import whu.edu.ljj.demos.ZmqSource;
//import whu.edu.ljj.flink.utils.LocationOP;
//import whu.edu.ljj.flink.utils.TrajePrinter;
//import whu.edu.ljj.flink.utils.Utils.Location;
//import whu.edu.ljj.flink.utils.Utils.TrajeData;
//import whu.edu.ljj.flink.utils.Utils.TrajePoint;
//import whu.edu.ljj.flink.utils.Utils.VehicleData;
//
//import java.io.IOException;
//import java.util.Date;
//import java.util.LinkedList;
//import java.util.Map;
//import java.util.concurrent.ConcurrentHashMap;
//
////29> Utils.TrajeData(SN=7460125, DEVICEIP=100.65.23.240, TIME=1741611816350, COUNT=1, TDATA=[Utils.TrajePoint(ID=22898, Carnumber=, Type=0, Scope=[0, 0], Speed=25.928545, Wayno=2, Tpointno=14413, Boolean=0, Direct=1)])
//public class xiaohanyingpredictFlink {
//    private static final int WINDOW_SIZE = 10;//用来预测的窗口大小
//    private static final Map<Integer, VehicleData> vehicleMap = new ConcurrentHashMap<>();
//    static int timeout=300;
//    private static final int TIMEOUT_MS = 2000; // 超时时间
//    private static int ssn=0;
//
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        // 1. 接收传感器数据流（包含正常数据和 SN=-1 的异常数据）
////        DataStream<TrajeData> trajeStream = env.addSource(new ZmqSource("tcp://100.65.62.82:8030"))
//        DataStream<TrajeData> trajeStream = env.addSource(new ZmqSource("tcp://*:5563"))
//                .map(new MapFunction<JSONObject, TrajeData>() {
//                    @Override
//                    public TrajeData map(JSONObject value) throws Exception {
//                        String trajeJSON = value.toString();
//                        if(!trajeJSON.equals("{}"))
//                            return JSON.parseObject(trajeJSON, TrajeData.class);
//                        else {
//                            TrajeData noData = new TrajeData();
//                            noData.setSN(-1);
//                            return noData;
//                        }
//                    }
//
//                });
//
//        trajeStream.flatMap(new FlatMapFunction<TrajeData, TrajePoint>() {
//                    boolean firstEnter=true;
//                    @Override
//                    public void flatMap(TrajeData data, Collector<TrajePoint> out) throws IOException {
//                        if(data.getSN() != -1&&data.getTDATA() != null && !data.getTDATA().isEmpty()){
//                            TrajePrinter printer = new TrajePrinter();
//                            printer.printOneJson(data);
//                            updatePointData(data); // 更新车辆数据
//                            if (firstEnter) {//如果是第一次进入
//                                ssn = data.getSN();//记录当前sn号
//                                firstEnter = false;//不再是第一次进入了
//                            }else{
//                                ssn=handleSequence(data.getSN(), ssn);//如果前一个sn跟当前sn相差不是1，则输出  丢包。因为要和前一个sn比较，所以不能是第一条进入直接进行比较
//                            }
//                        }else{
//                            ssn+=1;
//                            handleTimeoutPrediction(ssn); // 触发超时预测
//                        }
//                    }
//                })
//                .keyBy(point -> point.getID()) // 按道路编号分组
//                .print();
//        env.execute("flink zmq consumer");
//
//    }
//    private static void handleTimeoutPrediction(int sn) throws IOException {
//        long currentTime = System.currentTimeMillis();
//        for (Map.Entry<Integer,   VehicleData> entry : vehicleMap.entrySet()) {
//            long carId = entry.getKey();
//            VehicleData data = entry.getValue();
//            synchronized (data) {
//                if (currentTime - data.getLastReceivedTime() >= TIMEOUT_MS && !data.getSpeedWindow().isEmpty()) {
//                    // 使用车辆独立窗口计算
//                    Float predictedSpeed = calculateMovingAverage(data.getSpeedWindow());
//                    data.getSpeedWindow().addLast(predictedSpeed);
//                    data.getSpeedWindow().removeFirst();
//                    int distanceDiff = (int) (predictedSpeed * 0.2 ); // 米
//                    int newTpointno = data.getTpointno() + distanceDiff; // 更新里程点
//                    data.setTpointno(newTpointno);
////                long carid=data.carid;
//                    int carType= data.getCartype();
//                    // 输出预测结果
//                    Location location1= LocationOP.getLocation("D:\\learn\\codes\\a_idea_codes\\flinkTest\\src\\main\\java\\whu\\edu\\data\\孝汉应.json",newTpointno);
//                    Location location2= LocationOP.getLocation("D:\\learn\\codes\\a_idea_codes\\flinkTest\\src\\main\\java\\whu\\edu\\data\\孝汉应.json",newTpointno+(int)(calculateMovingAverage(data.getSpeedWindow())*0.2));
//                    String output = String.format(
//                            "[预测]SN：%d time:%tT Car ID:%d  Car Number:    Type:%d  Scope:[0,0]  Speed:%.14f m/s  经度:%.10f  纬度:%.10f  正北航向角（东90南180）：%.3f    Wayno:%d  Tpointno:%d  Boolean:0  Direct:%d (基于最近%d条数据)",
//                            sn,new Date(currentTime),carId, carType,predictedSpeed,location1.getLatitude(),location1.getLongitude(),calculateBearing(location1.getLatitude(),location1.getLongitude(),location2.getLatitude(),location2.getLongitude()), data.getWayno(), newTpointno, data.getDirect(), data.getSpeedWindow().size()
//                    );
//                    System.out.println(output);
//                }
//            }
//        }
//    }
//    public static double calculateBearing(double startLat, double startLon, double endLat, double endLon) {
//        // 将十进制度数转换为弧度
//        double lat1 = Math.toRadians(startLat);
//        double lon1 = Math.toRadians(startLon);
//        double lat2 = Math.toRadians(endLat);
//        double lon2 = Math.toRadians(endLon);
//
//        // 计算经度差
//        double dLon = lon2 - lon1;
//
//        // 计算方位角
//        double y = Math.sin(dLon) * Math.cos(lat2);
//        double x = Math.cos(lat1) * Math.sin(lat2)
//                - Math.sin(lat1) * Math.cos(lat2) * Math.cos(dLon);
//
//        double bearing = Math.atan2(y, x);
//
//        // 将弧度转换为度数（0-360范围）
//        bearing = Math.toDegrees(bearing);
//        bearing = (bearing + 360) % 360;
//
//        return bearing;
//    }
//    private static float calculateMovingAverage(LinkedList<Float> speedWindow) {
//        synchronized (speedWindow) {
//            return (float) speedWindow.stream()
//                    .mapToDouble(Float::doubleValue)
//                    .average()
//                    .orElse(Double.NaN);
//        }
//    }
//    private static void updatePointData( TrajeData TrajeData) {
//            long currentTime = System.currentTimeMillis();
//            for ( TrajePoint vehicle : TrajeData.getTDATA()) {//遍历当前这条TrajeData消息中的所有TrajePoint
//                VehicleData data = vehicleMap.computeIfAbsent(vehicle.getID(), k -> new  VehicleData());
//                synchronized (data) {
//                    // 更新车辆独立窗口（原data.speedWindow）
//                    data.getSpeedWindow().add(vehicle.getSpeed());
//                    if (data.getSpeedWindow().size() > WINDOW_SIZE) {
//                        data.getSpeedWindow().removeFirst();
//                    }
//                    data.setTpointno(vehicle.getTpointno());
//                    data.setLastReceivedTime(TrajeData.getTIME());
//                }
//            }
//        }
//    private static int handleSequence(int currentSN, int lastSN) {
//        if (currentSN != lastSN + 1) {
//            System.out.printf("丢包! 当前SN: %d, 期望SN: %d%n", currentSN, lastSN + 1);
//        }
//        return currentSN;
//    }
//}
//
//
//
