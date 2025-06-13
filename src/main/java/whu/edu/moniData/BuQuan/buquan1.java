package whu.edu.moniData.BuQuan;

import com.alibaba.fastjson2.JSON;
import com.github.luben.zstd.ZstdOutputStream;
import com.google.gson.Gson;
import javafx.util.Pair;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerConfig;
import whu.edu.ljj.flink.utils.JsonReader;
import whu.edu.ljj.flink.utils.myTools;
import whu.edu.ljj.flink.xiaohanying.Utils.Location;
import whu.edu.ljj.flink.xiaohanying.Utils.PathPoint;
import whu.edu.ljj.flink.xiaohanying.Utils.PathPointData;
import whu.edu.ljj.flink.xiaohanying.Utils.PathTData;
import whu.edu.moniData.Utils.TrafficEventUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

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
    //        60817                        62914                         62914
    //生产者端max.request.size必须小于集群的message.max.bytes以及消费者的max.partition.fetch.bytes
    //MergedPathData.sceneTest.1 "fiberDataTest1", "fiberDataTest2", "fiberDataTest3" 100.65.38.139:9092
    public static void main(String[] args) throws Exception {
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {
            env.setParallelism(3);
            String brokers = args[0];
            List<String> topics  = Arrays.asList("fiberData1","fiberData2","fiberData3","fiberData4","fiberData5","fiberData6","fiberData7","fiberData8","fiberData9","fiberData10","fiberData11");
            String groupId = "flink_consumer_group1";
            // 从Kafka读取数据
            KafkaSource<String> source = KafkaSource.<String>builder()
                    .setBootstrapServers(brokers)
                    .setTopics(topics)
                    .setGroupId(groupId)
                    .setStartingOffsets(OffsetsInitializer.latest())
                    .setValueOnlyDeserializer(new SimpleStringSchema())
//                    .setProperty("max.request.size", "608174080")
                    .setProperty("max.partition.fetch.bytes", "629145600")
                    .build();
            // 从Kafka读取数据
//            DataStreamSource<String> kafkaStream = env.fromSource(buildSource(brokers, topics), WatermarkStrategy.noWatermarks(), "Kafka Source1");
            DataStreamSource<String> kafkaStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source1");

//读取基站数据，返回StationStream
            DataStream<PathTData> StationStream=kafkaStream.flatMap((String jsonStr, Collector<PathTData> out)-> {
                try {
                    PathTData data1 = null;
//                    if(myTools.getNString(jsonStr,2,10).equals("pathList")) {
                    data1 = JSON.parseObject(jsonStr, PathTData.class);
                    out.collect(data1);
//                    }
                } catch (Exception e) {
                    System.err.println("JSON解析失败: "+ e.getMessage());
                }
            }).returns(PathTData.class).keyBy(PathTData::getTime);
            SingleOutputStreamOperator<PathTData> endPathTDataStream = StationStream.flatMap(new FlatMapFunction<PathTData, PathTData>() {
                @Override//5.56   33.76  86.64
                public void flatMap(PathTData pathTData, Collector<PathTData> collector) throws Exception {
                    List<PathPoint> list = new ArrayList<>();
                    PathTData pathTData1 = initResPathTDate(pathTData);
                    String ts = pathTData.getTimeStamp();
                    temp = initCurrentTime(ts);
//                    System.out.println(pathTData.getTimeStamp());
                    if (!pathTData.getPathList().isEmpty()) {
                        if (firstEnter) {
                            list = firstEnterInitializePointMap(pathTData);
                            pathTData1.setPathList(list);
                        } else {
                            putNowDataIntoTempMap(pathTData, ts);


                            for (Map.Entry<Long, PathPoint> entry : tempMap.entrySet()) {//当前的所有数据直接加入
                                PathPoint p = entry.getValue();
                                if (p.getStakeId() != null && p.getTimeStamp() != null) list.add(p);
                            }
//                        }
//                    }
//                                            pathTData1.setPathList(list);
//                        collector.collect(pathTData1);
//                        tempMap.clear();
//                }
//
                            System.out.println("tempMap.size():  "+tempMap.size()+"  content:"+tempMap);

                            //如果里程越界，会被移除
                            for (Map.Entry<Long, PathPointData> entry : pointMap.entrySet()) {
                                if (tempMap.get(entry.getKey()) == null) {//PointMap中有，但是当前tempMap中没有，车辆缺失
                                    PathPointData pdInPointMap = predictNextMixed(entry.getKey(), pathTData.getTimeStamp());
                                    if (pdInPointMap != null) {

                                        list.add(PDToPP(pdInPointMap));
                                        pointMap.put(pdInPointMap.getId(), pdInPointMap);
                                    }
                                }
                            }
                            //更新pointmap
//                            for(PathPoint p:pathTData.getPathList()) {
//                                long keyId=p.getId();
//                                if (pointMap.get(keyId) == null) {//即前面没有，当前有，是个新车。（会不会是重新出现的车呢）反正在这个if里是个绝对的新车
//                                    PathPointData ppp = PPToPDAndinitLastRecAndWindow(p);
//                                    pointMap.put(keyId, ppp);
//                                } else {//pointmap有，当前有，看前一条是不是预测的
//                                    if(  pointMap.get(keyId).getLastReceivedTime() ==1) {//说明前面是预测的
//                                        //重新出现，怎么处理？把前面的先全部删掉,然后再全部加入。那么会不会跟前面的《当前的所有数据直接加入》重复呢？不会，前面的没有改变pointmap，只是加入list
//                                        pointMap.remove(keyId);
//                                        PathPointData pdpd= PPToPD(p);
//                                        ConcurrentLinkedDeque<Float> c = new ConcurrentLinkedDeque<>();
//                                        c.add(p.getSpeed());
//                                        pdpd.setSpeedWindow(c);
//                                        pdpd.setLastReceivedTime(0);
//                                        pointMap.put(keyId,pdpd);
//                                    }else{//前面有，当前有，且前面的不是预测的
//                                        pointMap.get(keyId).getSpeedWindow().add(tempMap.get(keyId).getSpeed());
//                                    }
//                                }
//
//                            }

                        }
                        pathTData1.setPathList(list);
                        collector.collect(pathTData1);
                        tempMap.clear();
                        System.out.println("list.size():  "+list.size());
                        Set<Long> se=new HashSet<>();
                        List<String >s=new ArrayList<>();
//                        for(PathPoint p:list){
//                            se.add(p.getId());
////                            System.out.println(p);
//                            s.add(p.getPlateNo()+"  "+p.getStakeId());
//                        }
//                        System.out.println("se.size():  "+se.size());
                        System.out.println(s);
                    }//pathlist.empty
                }//flatMap
            });
            writeIntoKafka(endPathTDataStream);
            env.execute("Flink completion to Kafka1 : completed.pathdata");
        }//flink env

    }//main
    private static PathPointData PPToPDAndinitLastRecAndWindow(PathPoint pp){
        PathPointData pd=PPToPD(pp);
        ConcurrentLinkedDeque<Float> c = new ConcurrentLinkedDeque<>();
        c.add(pp.getSpeed());
        pd.setSpeedWindow(c);
        pd.setLastReceivedTime(0);
        return pd;
    }
    private static PathPointData predictNextMixed(long keyInPointMap,String timestamp){
        PathPointData pdInPointMap=pointMap.get(keyInPointMap);
        Pair<ConcurrentLinkedDeque<Float>,Float> a1=predictSpeedWindow(pdInPointMap);//速度窗口、预测的速度
        double[] a2=predictNewMileage(pdInPointMap,a1.getValue());//新里程、驶过的距离
        if(a2[0]<1016020||a2[1]>1173790){
            pointMap.remove(keyInPointMap);tempMap.remove(keyInPointMap);
            return null;
        }
        Pair<String,double[]> a3=predictStake(pdInPointMap,a2[0],a2[1]);//新桩号、新经纬度lonlng
        if(a3==null) {
            System.out.println("已移除 "+pdInPointMap.getId());
            return null;
        }

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
        pdInPointMap.setLastReceivedTime(1);

        if(!pdInPointMap.getPlateNo().endsWith("值"))pdInPointMap.setPlateNo(pdInPointMap.getPlateNo()+" "+"预测值");
        else{
            pdInPointMap.setPlateNo(pdInPointMap.getPlateNo().substring(0,7)+" "+"预测值");
        }

        PathPoint pp=PDToPP(pdInPointMap);
//        System.out.println(pdInPointMap.getId()+"  "+ pdInPointMap.getPlateNo());

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

    private static void putNowDataIntoTempMap(PathTData pathTData,String time){
        List<PathPoint> p=pathTData.getPathList();
        for(PathPoint m:p){
            m.setTimeStamp(time);
            tempMap.put(m.getId(),m);
        }

    }
    private static List<PathPoint> firstEnterInitializePointMap (PathTData pathTData){
        List<PathPoint> p=pathTData.getPathList();
        for(PathPoint m:p){
            PathPointData pp=PPToPD(m);
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
    public static void writeIntoKafka(SingleOutputStreamOperator<PathTData> endPathTDataStream) {
        // Kafka生产者配置
        Properties producerProps = new Properties();
        producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "100.65.38.40:9092");
        producerProps.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "10485760"); // 10MB
        producerProps.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd"); // ZSTD压缩
        producerProps.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "1048576"); // 1MB批处理大小

        // 动态分块配置
        final int MAX_UNCOMPRESSED_SIZE = 5 * 1024 * 1024; // 5MB原始数据阈值
        final int MIN_CHUNK_SIZE = 50; // 最小分块车辆数
        final int MAX_CHUNK_SIZE = 1000; // 最大分块车辆数
        final double TARGET_COMPRESSION_RATIO = 0.4; // 目标压缩比

        DataStream<String> jsonStream = endPathTDataStream
                .flatMap(new RichFlatMapFunction<PathTData, String>() {
                    private transient Gson gson;

                    @Override
                    public void open(Configuration parameters) {
                        gson = new Gson();
                        DateTimeFormatter timestampFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS");
                    }

                    @Override
                    public void flatMap(PathTData data, Collector<String> out) {
                        try {
                            // 1. 元数据提取
                            long baseTime = data.getTime();
                            String baseTimestamp = data.getTimeStamp();
                            int totalPoints = data.getPathList().size();

                            // 2. 动态分块策略
                            int chunkSize = calculateDynamicChunkSize(totalPoints);
                            List<List<PathPoint>> chunks = partitionData(data.getPathList(), chunkSize);

                            // 3. 分块处理
                            for (int i = 0; i < chunks.size(); i++) {
                                // 创建分块数据
                                PathTData chunkData = new PathTData();
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

                                // 日志记录
                                System.out.printf("发送分块 %d/%d | 车辆数: %d | 原始大小: %.2fMB | 压缩后: %.2fMB%n",
                                        i+1, chunks.size(), chunks.get(i).size(),
                                        jsonBytes.length / 1024.0 / 1024.0,
                                        (jsonBytes.length * TARGET_COMPRESSION_RATIO) / 1024.0 / 1024.0);
                            }
                        } catch (Exception e) {
                            System.err.println("消息处理出错: " + e.getMessage());
                            e.printStackTrace();
                        }
                    }

                    // 动态计算分块大小
                    private int calculateDynamicChunkSize(int totalPoints) {
                        if (totalPoints <= 1000) return Math.max(MIN_CHUNK_SIZE, totalPoints / 2);
                        if (totalPoints <= 5000) return 300;
                        if (totalPoints <= 10000) return 500;
                        return MAX_CHUNK_SIZE;
                    }

                    // 高效数据分区
                    private List<List<PathPoint>> partitionData(List<PathPoint> points, int chunkSize) {
                        List<List<PathPoint>> chunks = new ArrayList<>();
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
                .setBootstrapServers("100.65.38.40:9092")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("completed.pathdata")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setKafkaProducerConfig(producerProps)
                .build();

        jsonStream.sinkTo(sink);
    }
}//public buquan class