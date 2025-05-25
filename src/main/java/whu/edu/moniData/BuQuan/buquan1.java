package whu.edu.moniData.BuQuan;
import whu.edu.moniData.BuQuan.buquanji.*;
import com.alibaba.fastjson2.JSON;
import javafx.util.Pair;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import whu.edu.ljj.flink.utils.JsonReader;
import whu.edu.ljj.flink.utils.myTools;
import whu.edu.ljj.flink.xiaohanying.Utils.*;
import whu.edu.moniData.Utils.TrafficEventUtils;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static whu.edu.ljj.flink.utils.LocationOP.UseSKgetLL;
import static whu.edu.ljj.flink.utils.calAngle.calculateBearing;
import static whu.edu.moniData.BuQuan.buquanji.predictSpeedWindow;
import static whu.edu.moniData.BuQuan.buquanji.predictStake;


public class buquan1 {
    private static final int WINDOW_SIZE = 20;//用来预测的窗口大小
    private static final Map<Long, PathPointData> pointMap = new ConcurrentHashMap<>();
    static boolean firstEnter = true;
    static Map<Long, PathPoint> tempMap = new ConcurrentHashMap<>();
    private static String pathTimeStamp = "";
    private static long pathTime = 0;
    private static long temp = 0;
    private static TrafficEventUtils.MileageConverter mileageConverter1;
    private static TrafficEventUtils.MileageConverter mileageConverter2;
    private static TrafficEventUtils.StakeAssignment stakeAssign1;
    private static TrafficEventUtils.StakeAssignment stakeAssign2;
    static List<Location> roadAKDataList;
    static List<Location> roadBKDataList;
    static List<Location> roadCKDataList;
    static List<Location> roadDKDataList;
    static {
        try {
            mileageConverter1 = new TrafficEventUtils.MileageConverter("sx_json.json");
            mileageConverter2 = new TrafficEventUtils.MileageConverter("xx_json.json");
            stakeAssign1 = new TrafficEventUtils.StakeAssignment("sx_json.json");
            stakeAssign2 = new TrafficEventUtils.StakeAssignment("xx_json.json");
            roadAKDataList = JsonReader.readJsonFile("AK_locations.json");
            roadBKDataList = JsonReader.readJsonFile("BK_locations.json");
            roadCKDataList = JsonReader.readJsonFile("CK_locations.json");
            roadDKDataList = JsonReader.readJsonFile("DK_locations.json");

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    //MergedPathData.sceneTest.1 "fiberDataTest1", "fiberDataTest2", "fiberDataTest3" 100.65.38.139:9092
    public static void main(String[] args) throws Exception {
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {
            env.setParallelism(3);
            String brokers = args[0];
            List<String> topics  = Arrays.asList("MergedPathData.sceneTest.1");
            String groupId = "flink_consumer_group1";
            // 从Kafka读取数据
            KafkaSource<String> source = KafkaSource.<String>builder()
                    .setBootstrapServers(brokers)
                    .setTopics(topics)
                    .setGroupId(groupId)
                    .setStartingOffsets(OffsetsInitializer.latest())
                    .setValueOnlyDeserializer(new SimpleStringSchema())
                    .setProperty("message.max.bytes", "16777216")
                    .setProperty("max.partition.fetch.bytes", "16777216")
                    .build();
            // 从Kafka读取数据
//            DataStreamSource<String> kafkaStream = env.fromSource(buildSource(brokers, topics), WatermarkStrategy.noWatermarks(), "Kafka Source1");
            DataStreamSource<String> kafkaStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source1");

//读取基站数据，返回StationStream
            DataStream<PathTData> StationStream=kafkaStream.flatMap((String jsonStr, Collector<PathTData> out)-> {
                try {
                    StationData data =null;
                    PathTData data1 = null;
                    if(myTools.getNString(jsonStr,2,10).equals("pathList")) {
                        data1 = JSON.parseObject(jsonStr, PathTData.class);
                        out.collect(data1);
                    }
                } catch (Exception e) {
                    System.err.println("JSON解析失败: "+ e.getMessage());
                }
            }).returns(PathTData.class).keyBy(PathTData::getTime);
            SingleOutputStreamOperator<PathTData> endPathTDataStream = StationStream.flatMap(new FlatMapFunction<PathTData, PathTData>() {
                @Override//5.56   33.76  86.64
                public void flatMap(PathTData pathTData, Collector<PathTData> collector) throws Exception {
                    List<PathPoint> list=new ArrayList<>();
                    PathTData pathTData1 = initResPathTDate(pathTData);
                    temp = initCurrentTime(pathTData.getTimeStamp());
//                    System.out.println(pathTData.getTimeStamp());
                    if (!pathTData.getPathList().isEmpty()) {
                        if (firstEnter) {
                            list = firstEnterInitializePointMapAndlastMap(pathTData);
                            pathTData1.setPathList(list);
                        }
                        else {
                            putNowDataIntoTempMap(pathTData);
                            for(PathPoint p:pathTData.getPathList()){
                                for (Map.Entry<Long, PathPointData> entry : pointMap.entrySet()) {
                                    if(tempMap.get(entry.getKey())==null){//PointMap中有，但是当前tempMap中没有，车辆缺失
                                        PathPointData pdInPointMap=predictNextMixed(entry.getKey(),pathTData.getTimeStamp());
                                        list.add(PDToPP(pdInPointMap));
                                        pointMap.put(pdInPointMap.getId(), pdInPointMap);
                                    }
                                }
                                if(pointMap.get(p.getId())==null){//如果是个新车
                                    PathPointData ppp=newPD(p);
                                    pointMap.put(p.getId(),ppp);
                                    list.add(p);
                                }else{
                                    list.add(p);
                                }
                            }
                        }
                        pathTData1.setPathList(list);
                        collector.collect(pathTData1);
                        tempMap.clear();
                    }//pathlist.empty
                }//flatMap
            });
            writeIntoKafka(endPathTDataStream);
            env.execute("Flink completion to Kafka1 : completed.pathdata");
        }//flink env

    }//main
    private static PathPointData newPD(PathPoint pp){
        PathPointData pd=PPToPD(pp);
        pd.getSpeedWindow().add(pp.getSpeed());
        return pd;
    }
    private static PathPointData predictNextMixed(long keyInPointMap,String timestamp){
        PathPointData pdInPointMap=pointMap.get(keyInPointMap);
        Pair<LinkedList<Float>,Float> a1=predictSpeedWindow(pdInPointMap);//速度窗口、预测的速度
        double[] a2=predictNewMileage(pdInPointMap,a1.getValue());//新里程、驶过的距离
        Pair<String,double[]> a3=predictStake(pdInPointMap,a2[0],a2[1]);//新桩号、新经纬度lonlng
        double carangle=calculateBearing(a3.getValue()[1],a3.getValue()[0],pdInPointMap.getLatitude(),pdInPointMap.getLongitude());
        pdInPointMap.setCarAngle(carangle);
        pdInPointMap.setMileage((int) (a2[0]));
        pdInPointMap.setSpeed(a1.getValue());
//        pdInPointMap.setTimeStamp(pathTimeStamp);//未接收到，不更新
        pdInPointMap.setLatitude(a3.getValue()[1]);
        pdInPointMap.setLongitude(a3.getValue()[0]);
        pdInPointMap.setSpeedWindow(a1.getKey());
        pdInPointMap.setStakeId(a3.getKey());
        pdInPointMap.setTimeStamp(timestamp);
        PathPoint pp=PDToPP(pdInPointMap);
        return pdInPointMap;
    }


    public static double[] predictNewMileage(PathPointData data,float speed){

        double[]d={0,0};
        d[1] = myTools.calculateDistance(speed, 200);
        if(data.getDirection()==1) {
            d[0] = data.getMileage() + d[1]; // 更新里程点
        }else {
            d[0] = data.getMileage() - d[1]; // 更新里程点
        }
        //问题：新里程是否过大
        return d;
    }


    private static int stakeToMileage(String stakeId) {
        return Integer.parseInt(stakeId.split("\\+")[0].substring(1)) * 1000 + Integer.parseInt(stakeId.split("\\+")[1]);
    }
    private static String MileageToStake(int newMileage) {
        return newMileage/1000+"+"+(newMileage-(newMileage/1000*1000));
    }
    private static float calculateMovingAverage(LinkedList<Float> speedWindow) {
        return (float) speedWindow.stream()
                .mapToDouble(Float::doubleValue)
                .average()
                .orElse(Double.NaN);
    }
    private static void putNowDataIntoTempMap(PathTData pathTData){
        List<PathPoint> p=pathTData.getPathList();
        for(PathPoint m:p) tempMap.put(m.getId(),m);

    }
    private static List<PathPoint> firstEnterInitializePointMapAndlastMap (PathTData pathTData){
        List<PathPoint> p=pathTData.getPathList();
        for(PathPoint m:p){
            PathPointData pp=PPToPD(m);
            pp.setLastReceivedTime(pathTData.getTime());
            pp.getSpeedWindow().add(m.getSpeed());
            pointMap.put(m.getId(),pp);
        }
        firstEnter = false;
        return p;
    }
    private static PathPoint PDToPP(PathPointData Point) {
        PathPoint pathPoint = new PathPoint();

        pathPoint.setMileage(Point.getMileage());
        pathPoint.setId(Point.getId());
        pathPoint.setSpeed(Point.getSpeed());
        pathPoint.setDirection(Point.getDirection());
        pathPoint.setLatitude(Point.getLatitude());
        pathPoint.setLongitude(Point.getLongitude());
        pathPoint.setLaneNo(Point.getLaneNo());
        pathPoint.setCarAngle(Point.getCarAngle());
        pathPoint.setOriginalColor(Point.getOriginalColor());
        pathPoint.setPlateColor(Point.getPlateColor());
        pathPoint.setStakeId(Point.getStakeId());
        pathPoint.setPlateNo(Point.getPlateNo());
        pathPoint.setOriginalType(Point.getOriginalType());
        pathPoint.setVehicleType(Point.getVehicleType());
        pathPoint.setTimeStamp(Point.getTimeStamp());
        return pathPoint;
    }
    private static PathPointData PPToPD(PathPoint Point) {
        PathPointData pathPoint = new PathPointData();
        pathPoint.setMileage(Point.getMileage());
        pathPoint.setId(Point.getId());
        pathPoint.setSpeed(Point.getSpeed());
        pathPoint.setDirection(Point.getDirection());
        pathPoint.setLatitude(Point.getLatitude());
        pathPoint.setLongitude(Point.getLongitude());
        pathPoint.setLaneNo(Point.getLaneNo());
        pathPoint.setCarAngle(Point.getCarAngle());
        pathPoint.setOriginalColor(Point.getOriginalColor());
        pathPoint.setPlateColor(Point.getPlateColor());
        pathPoint.setStakeId(Point.getStakeId());
        pathPoint.setPlateNo(Point.getPlateNo());
        pathPoint.setOriginalType(Point.getOriginalType());
        pathPoint.setVehicleType(Point.getVehicleType());
        pathPoint.setTimeStamp(Point.getTimeStamp());
        pathPoint.setSpeedWindow(new LinkedList<>());

        return pathPoint;
    }
    public static long initCurrentTime(String time){
        try {
            // 尝试按三位毫秒格式解析
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS");
            LocalDateTime localDateTime = LocalDateTime.parse(time, formatter);
            return localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        } catch (Exception e) {
            // 若三位毫秒格式解析失败，尝试按两位毫秒格式解析
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SS");
            LocalDateTime localDateTime = LocalDateTime.parse(time, formatter);
            return localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        }
    }
    public static PathTData initResPathTDate(PathTData pathTData){
        pathTimeStamp=pathTData.getTimeStamp();
        pathTime= pathTData.getTime();
        PathTData pathTData1 = new PathTData();
        pathTData.setTime(pathTime);
        pathTData.setTimeStamp(pathTimeStamp);
        pathTData.setPathNum(pathTData1.getPathNum());
        pathTData.setWaySectionId(pathTData1.getWaySectionId());
        pathTData.setWaySectionName(pathTData1.getWaySectionName());
        return pathTData1;
    }
    public static void writeIntoKafka(SingleOutputStreamOperator<PathTData> endPathTDataStream){
        KafkaSink<String> dealStreamSink = KafkaSink.<String>builder()
                .setBootstrapServers("100.65.38.40:9092")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("completed.pathdata")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setProperty("max.request.size", "20971520") // 20MB
                .build();

        DataStream<String> jsonStream = endPathTDataStream
                .map(JSON::toJSONString)
                .returns(String.class);

        jsonStream.sinkTo(dealStreamSink);
    }
}//public buquan class