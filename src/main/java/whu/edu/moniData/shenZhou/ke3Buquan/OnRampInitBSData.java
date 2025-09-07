package whu.edu.moniData.shenZhou.ke3Buquan;

import com.alibaba.fastjson2.JSON;
import com.github.luben.zstd.ZstdOutputStream;
import com.google.gson.Gson;
import javafx.util.Pair;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerConfig;
import whu.edu.ljj.flink.utils.JsonReader;
import whu.edu.ljj.flink.utils.myTools;
import whu.edu.ljj.flink.xiaohanying.Utils;
import whu.edu.moniData.Utils.TrafficEventUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;

import static whu.edu.ljj.flink.utils.LocationOP.UseSKgetLL;
import static whu.edu.ljj.flink.utils.calAngle.calculateBearing;
import static whu.edu.moniData.BuQuan.buquanji.predictSpeedWindow;
import static whu.edu.moniData.BuQuan.buquanji.predictStake;
import static whu.edu.moniData.shenZhou.ke3Buquan.DataUtils.*;

/**
 * 7.31
 *  1.这里是不经过拼接和去重的原始基站数据，注意这里是整合过的，可以再写一个直接展示原数据的
 */

public class OnRampInitBSData {
    private static final int end_za=1460;
    private static final int start_za=692;
    private static final int WINDOW_SIZE = 10;//用来预测的窗口大小
    private static final Map<Long, Utils.PathPointData> pointMap = new ConcurrentHashMap<>();
    private static final Map<Long, Double> ifToPredict = new ConcurrentHashMap<>();
//                           车辆id   车辆里程
    static boolean firstEnter = true;
    static Map<Long, Utils.PathPoint> tempMap = new ConcurrentHashMap<>();
    private static long pathTime = 0;
    private static long temp = 0;
    private static String ts="";
    private static TrafficEventUtils.MileageConverter mileageConverter1;
    private static TrafficEventUtils.MileageConverter mileageConverter2;
    private static TrafficEventUtils.StakeAssignment stakeAssign1;
    private static TrafficEventUtils.StakeAssignment stakeAssign2;
    static List<Utils.Location> roadAKDataList;
    static List<Utils.Location> roadBKDataList;
    static List<Utils.Location> roadCKDataList;
    static List<Utils.Location> roadDKDataList;
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
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000); // 超时时间 60s
//        env.getCheckpointConfig().setCheckpointStorage("file:///d:/integrate");
        env.setParallelism(1);

        KafkaSource<String> fiberDataTestSource = KafkaSource.<String>builder()
                .setTopics("smartBS_xg")
                .setBootstrapServers("10.48.53.82:9092")
                .setGroupId("baseStation_xg")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setProperty("auto.offset.commit", "true")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<BaseStationData> bsDataStream = env.fromSource(fiberDataTestSource,
                        WatermarkStrategy.noWatermarks(),
                        "baseStation_XG Source")
                .setParallelism(1)
                .map(bsJSON -> JSON.parseObject(bsJSON, BaseStationData.class)).setParallelism(1);

        SingleOutputStreamOperator<BaseStationData> bsFDataStream = bsDataStream.process(new ProcessFunction<BaseStationData, BaseStationData>() {

            // 初始化经纬度-桩号转换器
            private transient KDTree kdTree;
            private transient Map<Integer, Double[]> bsLocationMap;

            @Override
            public void open(Configuration parameters) throws Exception {
                // JSON文件地址
                String jsonFilePath = "ABCDK_locations.json";
                // 基站位置excel文件
                String bsLocationFilePath = "bsLocation_xg_0724.xlsx";

                // 初始化状态
                try {
                    // KDTree
                    List<SpatialPoint> input = loadCheckpointsFromJSON(jsonFilePath);
                    kdTree = KDTree.build(input);

                    // bsLocationMap
                    bsLocationMap = loadCheckpointsFromEXCEL(bsLocationFilePath);
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void processElement(BaseStationData value, ProcessFunction<BaseStationData, BaseStationData>.Context ctx, Collector<BaseStationData> out) throws Exception {
                // 这里先过滤掉
                int devideId = value.getDeviceId();
                if (devideId == 3) {
                    List<BSPoint> fineBSPoints = new ArrayList<>();
                    for (BSPoint bsPoint : value.getParticipants()) {
                        double[] coordinate = new double[]{bsPoint.getLongitude(), bsPoint.getLatitude()};

                        if (RampStakeAssignment.calculateDistance(coordinate[0], coordinate[1], bsLocationMap.get(devideId)[0], bsLocationMap.get(devideId)[1]) * 1000 > 100.0)
                            continue;

                        SpatialPoint spResult = kdTree.query(coordinate);
                        double[] resultCoordinate = spResult.getCoordinate();
                        double distance = RampStakeAssignment.calculateDistance(coordinate[0], coordinate[1], resultCoordinate[0], resultCoordinate[1]) * 1000;
                        if (distance > 2.0) {
//                        System.out.println("此点："+bsPoint.getId()+"，距离匝道最近采样点："+distance+"m，大于阈值。");
                            continue;
                        }
                        String stake = "K1122+200-" + spResult.getLocation();
                        bsPoint.setRampStake(stake);
                        bsPoint.setLaneNo(spResult.getLaneNum());
                        bsPoint.setMileage(spResult.getLocationNum());
                        fineBSPoints.add(bsPoint);
                    }
                    value.setParticipants(fineBSPoints);

                    out.collect(value);
                }
            }
        });


        SingleOutputStreamOperator<Utils.PathTData> convertedStream = bsFDataStream.flatMap(
                new FlatMapFunction<BaseStationData, Utils.PathTData>() {
                    @Override
                    public void flatMap(BaseStationData bsData, Collector<Utils.PathTData> out) throws Exception {
                        Utils.PathTData pathTData = new Utils.PathTData();

                        // 设置时间相关字段
                        long timeInMillis = bsData.getTimestampMicrosec(); // 微秒转毫秒
                        pathTData.setTime(timeInMillis);

                        // 将时间戳转换为字符串格式
                        LocalDateTime dateTime = LocalDateTime.ofInstant(
                                Instant.ofEpochMilli(timeInMillis),
                                ZoneId.systemDefault()
                        );
                        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS");
                        String formattedDateTime = dateTime.format(formatter);
                        pathTData.setTimeStamp(formattedDateTime);

                        // 设置其他字段（没有的就填空）
                        pathTData.setWaySectionId(""); // 填空
                        pathTData.setWaySectionName(""); // 填空

                        // 转换参与者数据
                        List<Utils.PathPoint> pathPoints = new ArrayList<>();

                        for (BSPoint bsPoint : bsData.getParticipants()) {
                            Utils.PathPoint pathPoint = convert2PathPoint(bsPoint);

                            // 设置时间戳
                            pathPoint.setTimeStamp(formattedDateTime);

                            // 设置其他可能缺失的字段
                            pathPoint.setPlateColor(0); // 填空
                            pathPoint.setVehicleType(0); // 填空
                            pathPoint.setCarAngle(0.0); // 填空
                            pathPoint.setOriginalColor(0); // 填空
                            pathPoint.setWeight(0.0); // 填空
                            pathPoint.setEventinfo(null); // 填空
                            pathPoints.add(pathPoint);
                        }

                        pathTData.setPathList(pathPoints);
                        pathTData.setPathNum(pathPoints.size());

                        out.collect(pathTData);
                    }
                }
        ).name("Convert to PathTData");



        SingleOutputStreamOperator<Utils.PathTData> endPathTDataStream = convertedStream.flatMap(
                new FlatMapFunction<Utils.PathTData, Utils.PathTData>() {
                    @Override
                    public void flatMap(Utils.PathTData pathTData, Collector<Utils.PathTData> collector) throws Exception {
                        // 只处理非空数据
                        if (!pathTData.getPathList().isEmpty()) {
                            ts = pathTData.getTimeStamp();
                            temp = initCurrentTime(ts);
                            // 更新临时映射和点映射
                            putNowDataIntoTempMapAndPointMap(pathTData, ts);

                            // 遍历点映射中的所有车辆
                            for (Map.Entry<Long, Utils.PathPointData> entry : pointMap.entrySet()) {
                                long vehicleId = entry.getKey();
                                Utils.PathPointData pointData = entry.getValue();

                                // 检查车辆是否在预测范围内
                                if (pointData.getMileage() <= end_za / 2) {
                                    // 添加到预测映射
                                    ifToPredict.put(vehicleId, (double) pointData.getMileage());
                                } else {
                                    // 超出范围的车辆从点映射中移除
                                    pointMap.remove(vehicleId);
                                    ifToPredict.remove(vehicleId);
                                }
                            }

                            // 清空临时映射
                            tempMap.clear();
                        }

                        // 将原始数据传递到下游
                        collector.collect(pathTData);
                    }

                    // 更新临时映射和点映射的方法
                    private void putNowDataIntoTempMapAndPointMap(Utils.PathTData pathTData, String time) {
                        List<Utils.PathPoint> points = pathTData.getPathList();
                        for (Utils.PathPoint point : points) {
                            point.setTimeStamp(time);
                            long id = point.getId();
                            tempMap.put(id, point);

                            if (pointMap.containsKey(id)) {
                                // 更新现有车辆数据
                                pointMap.get(id).getSpeedWindow().add(point.getSpeed());
                                pointMap.get(id).setMileage(point.getMileage());
                                pointMap.get(id).setStakeId(point.getStakeId());
                                pointMap.get(id).setTimeStamp(point.getTimeStamp());

                            } else {
                                // 添加新车辆数据
                                pointMap.put(id, PPToPD(point));
                            }
                        }
                    }
                }
        ).name("Update ifToPredict");


        // 创建预测流
        DataStream<Utils.PathTData> predictionStream = createPredictionStream(env);

        // 合并两个流
        DataStream<Utils.PathTData> mergedStream = endPathTDataStream.union(predictionStream);

        // 将合并后的流写入Kafka
        writeIntoKafka1(mergedStream);
        DataStream<DataUtils.BaseStationData> speedOneStream = predictionStream.map(
                new MapFunction<Utils.PathTData, BaseStationData>() {
                    @Override
                    public BaseStationData map(Utils.PathTData value) throws Exception {
                        BaseStationData baseStationData = new BaseStationData();
                        if(value.getTimeStamp()!=null){
                            // 设置设备ID为3（模拟基站3）
                            baseStationData.setDeviceId(16);

                            // 设置时间戳（微秒）
                            baseStationData.setTimestampMicrosec(initCurrentTime(value.getTimeStamp())); // 毫秒转微秒

                            // 设置参与者数量
                            baseStationData.setParticipantCount(value.getPathList().size());

                            // 转换参与者数据
                            List<BSPoint> participants = new ArrayList<>();
                            for (Utils.PathPoint point : value.getPathList()) {
                                BSPoint bsPoint = new BSPoint();

                                // 设置基本属性
                                bsPoint.setId((int) point.getId());
                                bsPoint.setPlateNo(point.getPlateNo());
                                bsPoint.setLaneNo(point.getLaneNo());
                                bsPoint.setLongitude(point.getLongitude());
                                bsPoint.setLatitude(point.getLatitude());
                                bsPoint.setRampStake(point.getStakeId());
                                bsPoint.setMileage((double) point.getMileage());

                                // 将速度设置为1
                                bsPoint.setSpeed(point.getSpeed());

                                // 设置默认值
                                bsPoint.setType(point.getOriginalType()); // 默认类型
                                bsPoint.setColor(0); // 默认颜色
                                bsPoint.setSource(0); // 默认来源
                                bsPoint.setCameraId(0); // 默认摄像头ID
                                bsPoint.setAltitude(0.0f); // 默认海拔
                                bsPoint.setHeading(0.0f); // 默认方向

                                participants.add(bsPoint);
                            }

                            baseStationData.setParticipants(participants);
//                        System.out.println(baseStationData);
                        }
                        return baseStationData;

                    }
                }
        ).name("Convert to BaseStationData with Speed=1");

// 将速度设置为1的数据流写入bs16
        writeBaseStationDataToKafka16(speedOneStream);
        env.execute("base3 buquan (to bs3 and bs16)");
    }
    private static void writeBaseStationDataToKafka16(DataStream<DataUtils.BaseStationData> stream) {
        // 将BaseStationData转换为JSON字符串
        DataStream<String> jsonStream = stream.map(new MapFunction<DataUtils.BaseStationData, String>() {
            @Override
            public String map(DataUtils.BaseStationData value) throws Exception {
                return JSON.toJSONString(value);
            }

        });

        // Kafka生产者配置
        Properties producerProps = new Properties();
        producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.48.53.82:9092");
        producerProps.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "10485760"); // 10MB
        producerProps.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd"); // ZSTD压缩
        producerProps.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "1048576"); // 1MB批处理大小

        // Kafka Sink配置
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("10.48.53.82:9092")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("bs16") // 指定发送到bs16 topic
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setKafkaProducerConfig(producerProps)
                .build();

        jsonStream.sinkTo(sink).name("Speed One Prediction Kafka Sink (BaseStationData)");
    }
    private static void writeIntoKafka1(DataStream<Utils.PathTData> stream) {
        // 过滤掉PathList为空的数据
        DataStream<Utils.PathTData> filteredStream = stream.filter(new FilterFunction<Utils.PathTData>() {
            @Override
            public boolean filter(Utils.PathTData value) throws Exception {
                // 只保留PathList不为空的数据
                return value.getPathList() != null && !value.getPathList().isEmpty();
            }
        }).name("Filter Empty PathList");

        // 将PathTData转换为JSON字符串
        DataStream<String> jsonStream = filteredStream.map(new MapFunction<Utils.PathTData, String>() {
            @Override
            public String map(Utils.PathTData value) throws Exception {
                return JSON.toJSONString(value);
            }
        }).name("Convert to JSON");

        // Kafka生产者配置
        Properties producerProps = new Properties();
        producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.48.53.82:9092");
        producerProps.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "10485760"); // 10MB
        producerProps.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd"); // ZSTD压缩
        producerProps.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "1048576"); // 1MB批处理大小

        // Kafka Sink配置
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("10.48.53.82:9092")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("bs3") // 使用专门的topic
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setKafkaProducerConfig(producerProps)
                .build();

        jsonStream.sinkTo(sink).name("PathPoint Kafka Sink");
    }
    private static DataStream<Utils.PathTData> createPredictionStream(StreamExecutionEnvironment env) {
        return env.addSource(new SourceFunction<Utils.PathTData>() {
                    private volatile boolean isRunning = true;

                    @Override
                    public void run(SourceContext<Utils.PathTData> ctx) throws Exception {
                        // 在run方法内部创建formatter，避免序列化问题

                        while (isRunning) {
                            // 等待100ms
                            Thread.sleep(100);

                            // 检查ifToPredict是否为空
                            if (ifToPredict.isEmpty()) {
                                // 如果为空，跳过本次循环
                                continue;
                            }
                            // 创建空的PathTData对象
                            Utils.PathTData predictionData = new Utils.PathTData();

                            // 设置时间戳

                            // 创建预测点列表
                            List<Utils.PathPoint> predictedPoints = new ArrayList<>();

                            // 遍历需要预测的车辆
                            Iterator<Map.Entry<Long, Double>> iterator = ifToPredict.entrySet().iterator();
                            while (iterator.hasNext()) {
                                Map.Entry<Long, Double> entry = iterator.next();
                                Long vehicleId = entry.getKey();
                                Double currentMileage = entry.getValue();

                                // 获取车辆数据
                                Utils.PathPointData vehicleData = pointMap.get(vehicleId);
                                if (vehicleData == null) {
                                    iterator.remove();
                                    continue;
                                }

                                // 预测下一状态
                                Utils.PathPointData predictedData = predictNextMixed(vehicleId, "asd");
                                if (predictedData == null) {
                                    iterator.remove();
                                    pointMap.remove(vehicleId);
                                    continue;
                                }

                                // 更新里程
                                ifToPredict.put(vehicleId, (double) predictedData.getMileage());

                                // 检查是否超出范围
                                if (predictedData.getMileage() > (double) end_za /2) {
                                    iterator.remove();
                                    pointMap.remove(vehicleId);
                                } else {
                                    // 添加到预测点列表
                                    if(predictedData.getMileage()>=start_za) {
                                        predictedPoints.add(PDToPP(predictedData));
                                        predictionData.setTimeStamp(predictedData.getTimeStamp());

                                    }
                                }
                            }

                            // 设置预测数据
                            predictionData.setPathList(predictedPoints);
                            predictionData.setPathNum(predictedPoints.size());

                            // 收集数据

                            ctx.collect(predictionData);
                        }
                    }

                    @Override
                    public void cancel() {
                        isRunning = false;
                    }
                }).name("Vehicle Prediction Source")
                .setParallelism(1);
    }
    private static String add100MillisToTimestamp(String timestamp) {
        try {
            // 解析原始时间戳
            long millis = initCurrentTime(timestamp);
            // 增加100毫秒
            long newMillis = millis + 100;
            // 格式化为新时间戳
            LocalDateTime newDateTime = LocalDateTime.ofInstant(
                    Instant.ofEpochMilli(newMillis),
                    ZoneId.systemDefault()
            );
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS");
            return newDateTime.format(formatter);
        } catch (Exception e) {
            // 如果解析失败，返回原始时间戳
            return timestamp;
        }
    }
    public static Utils.PathPoint convert2PathPoint(BSPoint bsPoint) {
        Utils.PathPoint ppoint = new Utils.PathPoint();
        ppoint.setId(bsPoint.getId());
        ppoint.setMileage(bsPoint.getMileage() );
        ppoint.setLaneNo(bsPoint.getLaneNo());
        ppoint.setLatitude(bsPoint.getLatitude());
        ppoint.setLongitude(bsPoint.getLongitude());
        ppoint.setStakeId(bsPoint.getRampStake());
        ppoint.setPlateNo(bsPoint.getPlateNo());
        ppoint.setSpeed(keep2Digits(bsPoint.getSpeed()));
        ppoint.setOriginalType(bsPoint.getType());


//        ppoint.setOriginalColor(bsPoint.getColor());
//        ppoint.setCarAngle(bsPoint.getHeading());
//        ppoint.setOriginalType(bsPoint.getType());
        return ppoint;
    }
    public static double calculateDistance1(double speedKmh, int timeMs) {
        // 参数校验
        if (speedKmh < 0 || timeMs < 0) {
            System.out.println("speedKmh: "+speedKmh+"  time: "+timeMs);
            throw new IllegalArgumentException("速度和时间的值不能为负数");
        }
        return speedKmh * timeMs / 100000 ;
    }
    private static Utils.PathPointData predictNextMixed(long keyInPointMap, String timestamp){
//        System.out.println("进入预测吗，模块，key为"+keyInPointMap);
        Utils.PathPointData pdInPointMap=pointMap.get(keyInPointMap);
        String originalTimestamp = pdInPointMap.getTimeStamp();
        String newTimestamp = add100MillisToTimestamp(originalTimestamp);
//        System.out.println("车辆信息："+pdInPointMap);
        Pair<ConcurrentLinkedDeque<Double>,Double> a1=predictSpeedWindow(pdInPointMap);//速度窗口、预测的速度
        double[] a2=predictNewMileage(pdInPointMap,a1.getValue());//新里程、驶过的距离
//        System.out.println("新里程、驶过的距离:"+ Arrays.toString(a2));
        if(a2[0]>= (double) end_za /2){
            pointMap.remove(keyInPointMap);tempMap.remove(keyInPointMap);
            return null;
        }
        Pair<String,double[]> a3=predictStake(pdInPointMap,a2[0],a2[1]);//新桩号、新经纬度lonlng
//        System.out.println("新桩号:"+a3.getKey());
        if(a3==null) {
//            System.out.println("已移除 "+pdInPointMap.getId());
            return null;
        }

        double carangle=calculateBearing(a3.getValue()[1],a3.getValue()[0],pdInPointMap.getLatitude(),pdInPointMap.getLongitude());
        pdInPointMap.setCarAngle(carangle);
        pdInPointMap.setMileage(a2[0]);
        pdInPointMap.setSpeed( a1.getValue());
//        pdInPointMap.setTimeStamp(pathTimeStamp);//未接收到，不更新
        pdInPointMap.setLatitude(a3.getValue()[1]);
        pdInPointMap.setLongitude(a3.getValue()[0]);
        pdInPointMap.setSpeedWindow(a1.getKey());
        pdInPointMap.setStakeId(a3.getKey());
        pdInPointMap.setTimeStamp(newTimestamp);
        pdInPointMap.setLastReceivedTime(1);

        if(!pdInPointMap.getPlateNo().endsWith("值"))pdInPointMap.setPlateNo(pdInPointMap.getPlateNo()+" "+"预测值");
        else{
            pdInPointMap.setPlateNo(pdInPointMap.getPlateNo().substring(0,7)+" "+"预测值");
        }

        Utils.PathPoint pp=PDToPP(pdInPointMap);
//        System.out.println(pdInPointMap.getId()+"  "+ pdInPointMap.getPlateNo());
//        System.out.println("预测值："+pdInPointMap);
        return pdInPointMap;
    }
    public static double[] predictNewMileage(Utils.PathPointData data, double speed){

        double[]d={0,0};
        d[1] = calculateDistance1(speed, 100);
            d[0] = data.getMileage() + d[1]; // 更新里程点
        //问题：新里程是否过大
        return d;
    }
    private static void putNowDataIntoTempMapAndPointMap(Utils.PathTData pathTData, String time){
        List<Utils.PathPoint> p=pathTData.getPathList();
        for(Utils.PathPoint m:p){
            m.setTimeStamp(time);
            long id=m.getId();
            tempMap.put(id,m);
            if(pointMap.get(id)==null){
                pointMap.put(id,PPToPD(m));
            }else{
                pointMap.get(id).getSpeedWindow().add(m.getSpeed());
                pointMap.get(id).setMileage(m.getMileage());
                pointMap.get(id).setStakeId(m.getStakeId());
                ifToPredict.put(id, Double.valueOf(m.getMileage()));
            }
        }

    }
    private static List<Utils.PathPoint> firstEnterInitializePointMap (Utils.PathTData pathTData){
        List<Utils.PathPoint> p=pathTData.getPathList();
        for(Utils.PathPoint m:p){
            Utils.PathPointData pp=PPToPD(m);
            pp.setLastReceivedTime(0);
            pp.getSpeedWindow().add(m.getSpeed());
            String ts=pathTData.getTimeStamp();
            pp.setTimeStamp(ts);
            m.setTimeStamp(ts);
            pointMap.put(m.getId(),pp);
        }
        firstEnter = false;
        return p;
    }
    private static String MileageToStake(int newMileage) {
        return newMileage/1000+"+"+(newMileage-(newMileage/1000*1000));
    }
    public static Pair<String,double[]> predictStake(Utils.PathPointData data, double distance, double deta){
        String lastStake=data.getStakeId();
        String newStake="";
//        System.out.println("last:"+lastStake+"   deta:"+deta+"  id:"+data.getId());
        double[]d = new double[2];
        newStake="K1122+200-AK"+MileageToStake((int)distance);
        Utils.Location l=UseSKgetLL1(lastStake, roadAKDataList, deta,end_za);
        if(l==null){
            pointMap.remove(data.getId());tempMap.remove(data.getId());
            return null;
        }else{
            d[0]= l.getLongitude();
            d[1]= l.getLatitude();
        }
        return new Pair<>(newStake,d);
    }
    public static Utils.Location UseSKgetLL1(String sk, List<Utils.Location> roadlist, Double distance, int num)  {
        int j=0;
        String suffix="";
            int index = sk.lastIndexOf('-');
            if (index >= 0 && index < sk.length() - 1) {
                suffix = sk.substring(index + 1);
            }
        for(Utils.Location l:roadlist){
            if(l.getLocation().equals(suffix))
            {
                int a=((int) Math.ceil(distance))*2+j;
                if(a<=num) return roadlist.get(a);
            }
            else j++;
        }
        return null;
    }
    private static Utils.PathPoint PDToPP(Utils.PathPointData Point) {
        Utils.PathPoint pathPoint = new Utils.PathPoint();

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
    private static Utils.PathPointData PPToPD(Utils.PathPoint Point) {
        Utils.PathPointData pathPoint = new Utils.PathPointData();
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
        pathPoint.setSpeedWindow(new ConcurrentLinkedDeque<>());

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

    public static Utils.PathTData initResPathTDate(Utils.PathTData pathTData){
        String pathTimeStamp = pathTData.getTimeStamp();
        pathTime= pathTData.getTime();
        Utils.PathTData pathTData1 = new Utils.PathTData();
        pathTData.setTime(pathTime);
        pathTData.setTimeStamp(pathTimeStamp);
        pathTData.setPathNum(pathTData1.getPathNum());
        pathTData.setWaySectionId(pathTData1.getWaySectionId());
        pathTData.setWaySectionName(pathTData1.getWaySectionName());
        return pathTData1;
    }
    public static void writeIntoKafka(SingleOutputStreamOperator<Utils.PathTData> endPathTDataStream) {
        // Kafka生产者配置
        Properties producerProps = new Properties();
        producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.48.53.82:9092");
        producerProps.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "10485760"); // 10MB
        producerProps.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd"); // ZSTD压缩
        producerProps.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "1048576"); // 1MB批处理大小

        // 动态分块配置
        final int MAX_UNCOMPRESSED_SIZE = 5 * 1024 * 1024; // 5MB原始数据阈值
        final int MIN_CHUNK_SIZE = 50; // 最小分块车辆数
        final int MAX_CHUNK_SIZE = 1000; // 最大分块车辆数
        final double TARGET_COMPRESSION_RATIO = 0.4; // 目标压缩比

        DataStream<String> jsonStream = endPathTDataStream
                .flatMap(new RichFlatMapFunction<Utils.PathTData, String>() {
                    private transient Gson gson;

                    @Override
                    public void open(Configuration parameters) {
                        gson = new Gson();
                        DateTimeFormatter timestampFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS");
                    }

                    @Override
                    public void flatMap(Utils.PathTData data, Collector<String> out) {
                        try {
                            // 1. 元数据提取
                            long baseTime = data.getTime();
                            String baseTimestamp = data.getTimeStamp();
                            int totalPoints = data.getPathList().size();

                            // 2. 动态分块策略
                            List<List<Utils.PathPoint>> chunks = partitionData(data.getPathList(), totalPoints);

                            // 3. 分块处理
                            for (int i = 0; i < chunks.size(); i++) {
                                // 创建分块数据
                                Utils.PathTData chunkData = new Utils.PathTData();
                                chunkData.setTime(baseTime);
                                chunkData.setTimeStamp(baseTimestamp);
                                chunkData.setPathNum(chunks.get(i).size());
                                chunkData.setPathList(chunks.get(i));

                                // 序列化并压缩
                                String json = gson.toJson(chunkData);
                                byte[] jsonBytes = json.getBytes(StandardCharsets.UTF_8);

                                if (jsonBytes.length > MAX_UNCOMPRESSED_SIZE) {
                                    // 大块数据使用ZSTD压缩
                                    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                                         ZstdOutputStream zos = new ZstdOutputStream(bos)) {
                                        zos.write(jsonBytes);
                                        zos.close();
                                        byte[] compressed = bos.toByteArray();
                                        out.collect("ZSTD:" + Base64.getEncoder().encodeToString(compressed));
                                    }
                                } else {
                                    // 小块数据直接发送
                                    out.collect(json);
                                }

//                                // 日志记录
//                                System.out.printf("发送分块 %d/%d | 车辆数: %d | 原始大小: %.2fMB | 压缩后: %.2fMB%n",
//                                        i+1, chunks.size(), chunks.get(i).size(),
//                                        jsonBytes.length / 1024.0 / 1024.0,
//                                        (jsonBytes.length * TARGET_COMPRESSION_RATIO) / 1024.0 / 1024.0);
                            }
                        } catch (Exception e) {
                            System.err.println("消息处理出错: " + e.getMessage());
                            e.printStackTrace();
                        }
                    }



                    // 高效数据分区
                    private List<List<Utils.PathPoint>> partitionData(List<Utils.PathPoint> points, int chunkSize) {
                        List<List<Utils.PathPoint>> chunks = new ArrayList<>();
                        int from = 0;
                        while (from < points.size()) {
                            int to = Math.min(from + chunkSize, points.size());
                            chunks.add(new ArrayList<>(points.subList(from, to)));
                            from = to;
                        }
                        return chunks;
                    }
                })
                .returns(String.class);

        // 构建Kafka Sink
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("10.48.53.82:9092")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("bs3")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setKafkaProducerConfig(producerProps)
                .build();

        jsonStream.sinkTo(sink);
    }
    public static float keep2Digits(double number) {
        return (float) (Math.round(number * 100.0) / 100.0);
    }
}
