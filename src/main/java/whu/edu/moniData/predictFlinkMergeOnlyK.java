package whu.edu.moniData;

import com.alibaba.fastjson2.JSON;
import javafx.util.Pair;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import whu.edu.ljj.flink.utils.myTools;
import whu.edu.moniData.Utils.TrafficEventUtils;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static whu.edu.ljj.flink.xiaohanying.Utils.*;

public class predictFlinkMergeOnlyK {
    private static final int WINDOW_SIZE = 20;//用来预测的窗口大小
    private static final Map<Long, PathPointData> PointMap = new ConcurrentHashMap<>();
    private static final Map<Long, PathPointData> JizhanPointMap = new ConcurrentHashMap<>();//雷视数据获取到的匝道上的所有车
    static boolean firstEnter=true;
    static Map<Long,Pair<Boolean,Integer>> nowMap= new ConcurrentHashMap<>();
    //    carid  是否在路上  数据缺失了几次
    //       carid  carNumber  匝道编号
    private static final long mainRoadMinMillage=0;//主路上的最小里程
    private static final long mainRoadMaxMillage=1111111111;//主路上的最大里程
    private static String pathTimeStamp="";
    private static float predictedSpeed=0;//预测速度
    private static double distanceDiff=0;
    private static long pathTime=0;
    private static int tcount=0;
    private static long t1=0;
    private static boolean tb1=true;
    private static boolean tb2=true;
    private static long t2=0;
    private static long t3=0;
    private static long temp=0;
    private static long dis=0;
    private static int newscount=0;
    private static TrafficEventUtils.MileageConverter mileageConverter1;
    private static TrafficEventUtils.MileageConverter mileageConverter2;
    private static TrafficEventUtils.StakeAssignment stakeAssign1;
    private static TrafficEventUtils.StakeAssignment stakeAssign2;

    static {
        try {
            mileageConverter1 = new TrafficEventUtils.MileageConverter("sx_json.json");
            mileageConverter2 = new TrafficEventUtils.MileageConverter("xx_json.json");
            stakeAssign1 = new TrafficEventUtils.StakeAssignment("sx_json.json");
            stakeAssign2 = new TrafficEventUtils.StakeAssignment("xx_json.json");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    //1、删除track的处理 2、
    public static void main(String[] args) throws Exception {

        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {
            env.setParallelism(3);
            // 配置Kafka连接信息
            String brokers = "100.65.38.139:9092";
            String groupId = "flink_consumer_group";
            List<String> topics = Arrays.asList("fiberDataTest1","fiberDataTest2","fiberDataTest3");
//            List<String> topics = Collections.singletonList("news-topic");
//            List<String> topics = Collections.singletonList("MergedPathData.sceneTest.1");
            // 创建Kafka数据源
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
            DataStreamSource<String> kafkaStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source1");
            DataStream<PathTData> parsedStream = kafkaStream
                    .flatMap((String jsonStr, Collector<PathTData> out) -> {
                        try {
                            PathTData data =null;
                            //验证，如果json的前几位是timestamp，则认为是mergedata
                                data = JSON.parseObject(jsonStr, PathTData.class);
                                out.collect(data);
                            System.out.println("data:" +data);
//                            }
                        } catch (Exception e) {
                            System.err.println("JSON解析失败: " + jsonStr);
                        }
                    }).returns(PathTData.class).keyBy(PathTData::getTime);
            SingleOutputStreamOperator<PathTData> endPathTDataStream=parsedStream.flatMap(new FlatMapFunction<PathTData, PathTData>() {
                @Override//5.56   33.76  86.64
                public void flatMap(PathTData PathTData, Collector<PathTData> collector) throws Exception {
//                    System.out.println(myTools.toDateTimeString(System.currentTimeMillis()));
                    newscount++;
                    try {
                        // 尝试按三位毫秒格式解析
                        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS");
                        LocalDateTime localDateTime = LocalDateTime.parse(PathTData.getTimeStamp(), formatter);
                        temp = localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
                    } catch (Exception e) {
                        // 若三位毫秒格式解析失败，尝试按两位毫秒格式解析
                        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SS");
                        LocalDateTime localDateTime = LocalDateTime.parse(PathTData.getTimeStamp(), formatter);
                        temp = localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
                    }
                    t1 = System.currentTimeMillis();
                    if(firstEnter) t3=temp;
                    else {dis+=temp-t3;t3=temp;}
                    pathTimeStamp=PathTData.getTimeStamp();
                    pathTime= PathTData.getTime();
                    PathTData pathTData = new PathTData();
                    pathTData.setTime(pathTime);
                    pathTData.setTimeStamp(pathTimeStamp);
                    pathTData.setPathNum(PathTData.getPathNum());
                    pathTData.setWaySectionId(PathTData.getWaySectionId());
                    pathTData.setWaySectionName(PathTData.getWaySectionName());
                    List<PathPoint> list=new ArrayList<>();
                    //如果mergedata合法
                    if (!PathTData.getPathList().isEmpty()) {
                        //存车辆id对应的车辆是否在车道上、几次没有出现
                        Map<Long,Pair<Boolean,Integer>> tempMap= new ConcurrentHashMap<>();

//                        b=true;
                        updateMergePoint(PathTData);//更新当前车辆map
                        List<PathPoint> p=PathTData.getPathList();
                        for (PathPoint m : p) {
                            if (firstEnter) {//第一次有数据，初始化nowmap
                                FirstEnterputNowMap(PathTData);
                                tempMap = nowMap;
                                firstEnter = false;
                            } else {
                                tempMap.put(m.getId(), new Pair<>(true, 0));
                            }
                        }
                        newscount++;
                        //不是第一次有数据，已经初始化nowmap，遍历nowmap，看是否有车没了,也就是nowmap有，目前没有
                        //里程+数据丢失检测 上匝道

                        PointMap.forEach((k,v)->{
                            if(v.getMileage()==null)PointMap.remove(k);
                        });
                        JizhanPointMap.forEach((k,v)->{
                            if(v.getMileage()==null)JizhanPointMap.remove(k);
                        });
                        for (Map.Entry<Long, Pair<Boolean,Integer>> entry : nowMap.entrySet()) {
                            long key = entry.getKey();
                            Pair<Boolean, Integer> now = nowMap.get(key);
                            PathPointData pathPointData = PointMap.get(key);
                            if(pathPointData!=null) {
                                if (tempMap.get(key) == null && pathPointData.getMileage() != null) {//如果当前这辆车目前没有但是nowmap有，视为可能缺失。
                                    // 上面pathPointData.getMileage()!=null是因为模拟数据有的没mileage
                                    if (now.getKey()) {//==true就是还是三次以下 ==false就是已经消失三次以上，视为没了
                                        tempMap.put(key, new Pair<>(true, now.getValue() + 1));
                                        if (tempMap.get(key).getValue() == 1) {
                                            tempMap.put(key, new Pair<>(false, now.getValue()));
//                                        mark:移出zaMap的逻辑
                                        }
                                    } else tempMap.put(key, new Pair<>(false, now.getValue() + 1));
                                }
                            }
                        }
                        for (Map.Entry<Long, Pair<Boolean,Integer>> entry : tempMap.entrySet()) {
                            long key =entry.getKey();
                            PathPointData pathPointData =PointMap.get(key);
                            if(pathPointData!=null) {
                                if (pathPointData.getStakeId() != null) {
                                    String nString = myTools.getNString(pathPointData.getStakeId(), 0, 2);
                                    switch (nString) {
                                        case "AK":
                                        case "BK":
                                        case "CK":
                                        case "DK":
                                            tempMap.put(key,new Pair<>(false,3));
                                            break;
                                        case "K":
                                                tempMap.put(key,new Pair<>(true,0));
                                                nowMap.put(key,new Pair<>(true,0));
                                            break;
                                    }
                                }
                            }
                        }
                        for (Map.Entry<Long, PathPointData> entry : JizhanPointMap.entrySet()) {
                            long key =entry.getKey();
                            PathPointData pathPointData =PointMap.get(key);
                            if(pathPointData!=null) {
                                String nString = myTools.getNString(PointMap.get(key).getStakeId(), 0, 2);
                                if (nString.equals("K")) {
                                    tempMap.put(key, new Pair<>(true, 0));
                                    nowMap.put(key, new Pair<>(true, 0));
                                }
                                }
                            }

                        tempMap.forEach((key, value) -> {
                            PathPointData pd= PointMap.get(key);
                            if(pd!=null) {
                                //预测的点要加上：mileage、speed、skateID

                                    //因为数据正确，进来的时候就已经改了pointmap，无需再改
                                    //如果StakeId为空，则计算经纬度，如果经纬度为空，则计算stakeid
                                    String sk = pd.getStakeId();
                                    //如果pd的桩号为null，则计算出经纬度，如果不为null,则通过经纬度计算出桩号
                                    if (sk == null) {
                                        //上一个点的location（因为pointmap里存的是上一个点的，现在要预测当前点的）
                                        TrafficEventUtils.StakeAssignment   ts=(pd.getDirection() == 1)
                                                ? stakeAssign1 : stakeAssign2;
                                        String stake = ts.findInsertionIndex(pd.getLongitude(),pd.getLatitude());
                                        pd.setStakeId(stake);
                                    } else {//sk !== null
                                        //question
                                        if (pd.getLatitude() == 0) {
                                            //应该在主路，桩号却在匝道上，说明数据有问题
                                            if (!(myTools.getNString(sk, 0, 1)).equals("K")) return;
                                            TrafficEventUtils.MileageConverter converter = (pd.getDirection() == 1)
                                                    ? mileageConverter1 : mileageConverter2;
                                            double[] d=converter.findCoordinate(stakeToMileage(sk)+(int)distanceDiff).getLnglat();
                                            pd.setLatitude(d[1]);
                                            pd.setLongitude(d[0]);
                                        }
                                    }
                                    PathPoint pathPoint = PDToPP(pd);
                                    list.add(pathPoint);
                                    myTools.printPathPoint(pathPoint);
                            }//tempmap的key去取pointmap不为null，最大那个没了

                        });
                        //整个writter
                        pathTData.setPathList(list);
                        collector.collect(pathTData);
                        //mark:防撞
                        nowMap=tempMap;
                    }
                    System.out.println("fini");
                    if(newscount>100){
                        t2 = System.currentTimeMillis();
                        System.out.println("time split:"+(t2-t1-dis));
                    }

                }

            });
//            // 执行任务
            env.execute("Flink Read Kafka");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    //匝道上的预测

    //主路上的预测
    private static PathPoint PredictNextOne(PathPointData data) throws IOException {
        data.getSpeedWindow().addLast(predictedSpeed);
        data.getSpeedWindow().removeFirst();
        double newTpointno=0;
        if(data.getDirection()==1) {
            newTpointno = data.getMileage() + distanceDiff; // 更新里程点
        }else {
            newTpointno = data.getMileage() - distanceDiff; // 更新里程点
        }
        if(newTpointno<mainRoadMinMillage||newTpointno>mainRoadMaxMillage)return null;
        data.setMileage((int)newTpointno);
        long carid = data.getId();
        data.setSpeed(predictedSpeed);
        data.setTimeStamp(pathTimeStamp);
        //如果StakeId为空，则计算经纬度，如果经纬度为空，则计算stakeid
        String sk=data.getStakeId();

        //没有桩号有经纬度的情况
        if(sk==null){
            //用上一点的经纬度返回上一个点的location和在roadlist的第几个
            TrafficEventUtils.StakeAssignment   ts=(data.getDirection() == 1)
                    ? stakeAssign1 : stakeAssign2;
            String stake = ts.findInsertionIndex(data.getLongitude(), data.getLatitude());
            //加上里程后的下一个location，也就是当前点的预测location
            String newStake=MileageToStake((int)(stakeToMileage(stake)+distanceDiff));
            data.setStakeId(newStake);
            TrafficEventUtils.MileageConverter converter = (data.getDirection() == 1)
                    ? mileageConverter1 : mileageConverter2;
            double[] d=converter.findCoordinate(stakeToMileage(newStake)).getLnglat();
            data.setLatitude(d[1]);
            data.setLongitude(d[0]);
        }
        //没有经纬度有桩号的情况
        else{
            //如果不是主路，直接输出（说明数据有问题）
            if(!(myTools.getNString(sk,0,1)).equals("K"))return null;
            //用当前的skateID获取下一个点的经纬度，也就是当前点的经纬度
            //问题：distanceDiff是不是应该可能为负
            TrafficEventUtils.MileageConverter converter = (data.getDirection() == 1)
                    ? mileageConverter1 : mileageConverter2;
            double[] d=converter.findCoordinate(stakeToMileage(sk)+(int)distanceDiff).getLnglat();
            data.setLatitude(d[1]);
            data.setLongitude(d[0]);
            TrafficEventUtils.StakeAssignment   ts=(data.getDirection() == 1)
                    ? stakeAssign1 : stakeAssign2;
            data.setStakeId(ts.findInsertionIndex(data.getLongitude(), data.getLatitude()));
        }
//        mark:孝汉应==89，别的的话还得改
        double carangle=89;
        data.setCarAngle(carangle);
        PathPoint p=new PathPoint(data.getDirection(), data.getId(),data.getLaneNo(),(int) newTpointno, data.getPlateNo(), data.getSpeed(),data.getTimeStamp(), data.getPlateColor(), data.getVehicleType(),data.getLongitude(),data.getLatitude(),data.getCarAngle(),data.getStakeId(),  data.getOriginalType(), data.getPlateColor());
        updateOnePointMap(p);
        myTools.printmergePoint(p);
        return p;
    }
    private static PathPoint predicOne(PathPointData PathPointData){
        return null;
    }
    //防撞，如果快碰到前车，则将其speedwindow全部-10
//    private static void deSpeedWindow(PathPointData PathPointData){
//        LinkedList<Float> speedWindow = PathPointData.getSpeedWindow();
//        ListIterator<Float> iterator = speedWindow.listIterator();
//        while (iterator.hasNext()) {
//            float originalValue = iterator.next();
//            iterator.set(originalValue - 10);
//        }
//    }

    private static float calculateMovingAverage(LinkedList<Float> speedWindow) {
        return (float) speedWindow.stream()
                .mapToDouble(Float::doubleValue)
                .average()
                .orElse(Double.NaN);
    }
    private static int stakeToMileage(String stakeId) {
        if (stakeId == null) {
            throw new IllegalArgumentException("Stake ID cannot be null");
        }

        String[] parts = stakeId.split("\\+");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid stake format: " + stakeId);
        }

        String prefix = parts[0];
        if (!prefix.startsWith("K") || prefix.length() < 2) {
            throw new IllegalArgumentException("Stake must start with 'K' followed by numbers: " + stakeId);
        }

        try {
            int km = Integer.parseInt(prefix.substring(1));
            int meter = Integer.parseInt(parts[1]);
            return km * 1000 + meter;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid number in stake ID: " + stakeId, e);
        }
    }
    private static String MileageToStake(int newMileage) {
        return newMileage/1000+"+"+(newMileage-(newMileage/1000*1000));
    }
    private static void FirstEnterputNowMap(PathTData PathTData) throws IOException {
        List<PathPoint> p=PathTData.getPathList();
        for(PathPoint m:p){
            nowMap.put(m.getId(),new Pair<>(true,0));
        }
    }
    private static void updateMergePoint(PathTData PathTData) {
        for (PathPoint Point : PathTData.getPathList()) {//遍历当前这条TrajeData消息中的所有TrajePoint
            PathPointData data = PointMap.compute(Point.getId(), (k,v) -> new PathPointData());
            synchronized (data) {
                data.getSpeedWindow().add(Point.getSpeed());
                if (data.getSpeedWindow().size() > WINDOW_SIZE) {
                    data.getSpeedWindow().removeFirst();
                }
                data.setMileage(Point.getMileage());
                data.setId(Point.getId());
                data.setSpeed(Point.getSpeed());
                data.setDirection(Point.getDirection());
                data.setLatitude(Point.getLatitude());
                data.setLongitude(Point.getLongitude());
                data.setLaneNo(Point.getLaneNo());
                data.setCarAngle(Point.getCarAngle());
                data.setOriginalColor(Point.getOriginalColor());
                data.setPlateColor(Point.getPlateColor());
                data.setStakeId(Point.getStakeId());
                data.setPlateNo(Point.getPlateNo());
                data.setOriginalType(Point.getOriginalType());
                data.setLastReceivedTime(PathTData.getTime());
                data.setTimeStamp(Point.getTimeStamp());
                data.setVehicleType(Point.getVehicleType());
            }
//            if(PointMap.get(Point.getId()).getMileage()==null)PointMap.remove(Point.getId());

        }
    }
    //没有更改lastreceivedtime，因为用在预测里，没有真的接受到
    private static void updateOnePointMap(PathPoint Point) {

        PathPointData data = PointMap.compute(Point.getId(), (k,v) -> new PathPointData());
        synchronized (data) {
            data.getSpeedWindow().add(Point.getSpeed());
            if (data.getSpeedWindow().size() > WINDOW_SIZE) {
                data.getSpeedWindow().removeFirst();
            }
            data.setMileage(Point.getMileage());
            data.setId(Point.getId());
            data.setSpeed(Point.getSpeed());
            data.setDirection(Point.getDirection());
            data.setLatitude(Point.getLatitude());
            data.setLongitude(Point.getLongitude());
            data.setLaneNo(Point.getLaneNo());
            data.setCarAngle(Point.getCarAngle());
            data.setOriginalColor(Point.getOriginalColor());
            data.setPlateColor(Point.getPlateColor());
            data.setStakeId(Point.getStakeId());
            data.setPlateNo(Point.getPlateNo());
            data.setOriginalType(Point.getOriginalType());
            data.setTimeStamp(Point.getTimeStamp());
            data.setVehicleType(Point.getVehicleType());
        }
        if(PointMap.get(Point.getId()).getMileage()==null)PointMap.remove(Point.getId());

    }
    private static void updateOneJizhanPointMap(PathPoint Point) {

        PathPointData data = JizhanPointMap.compute(Point.getId(), (k,v) -> new PathPointData());
        synchronized (data) {
            data.getSpeedWindow().add(Point.getSpeed());
            if (data.getSpeedWindow().size() > WINDOW_SIZE) {
                data.getSpeedWindow().removeFirst();
            }
            data.setMileage(Point.getMileage());
            data.setId(Point.getId());
            data.setSpeed(Point.getSpeed());
            data.setDirection(Point.getDirection());
            data.setLatitude(Point.getLatitude());
            data.setLongitude(Point.getLongitude());
            data.setLaneNo(Point.getLaneNo());
            data.setCarAngle(Point.getCarAngle());
            data.setOriginalColor(Point.getOriginalColor());
            data.setPlateColor(Point.getPlateColor());
            data.setStakeId(Point.getStakeId());
            data.setPlateNo(Point.getPlateNo());
            data.setOriginalType(Point.getOriginalType());
            data.setTimeStamp(Point.getTimeStamp());
            data.setVehicleType(Point.getVehicleType());
        }
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
}
