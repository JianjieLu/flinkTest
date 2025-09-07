package whu.edu.moniData.shenZhou;

import com.alibaba.fastjson2.JSON;
import com.github.luben.zstd.ZstdOutputStream;
import com.google.gson.Gson;
import javafx.util.Pair;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import whu.edu.ljj.flink.utils.JsonReader;
import whu.edu.ljj.flink.utils.myTools;
import whu.edu.moniData.Utils.TrafficEventUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;


import static whu.edu.ljj.flink.utils.calAngle.calculateBearing;
import static whu.edu.moniData.BuQuan.buquanji.predictSpeedWindow;
import static whu.edu.moniData.BuQuan.buquanji.predictStake;
import static whu.edu.moniData.shenZhou.Utils.*;


/**
 * Special Edition new-Local for simulation
 * 7.3
 *  测试使得分区数等于并行数量
 * 7.8
 *  1.使得key分区与subTask数量一致
 *  2.使用RedisSink
 * 7.16
 *  1.Kafka数据源改成 10.48.53.82，停止写入，并开始测试合并上牌结果（传给下游补全任务）
 *  2.设置过期时间
 *  3.合并所有路段
 *
 */
public class TGCTime1stLocalSV5P6 {
    // 定义侧输出流
    private static final OutputTag<Tuple2<String, String>> VS_TAG = new OutputTag<>("vs", Types.TUPLE(Types.STRING, Types.STRING));

    private static final OutputTag<Tuple2<String, String>> FM_TAG = new OutputTag<>("fm", Types.TUPLE(Types.STRING, Types.STRING));
    private static final int WINDOW_SIZE = 20; // 用来预测的窗口大小
    private static final Map<Long, whu.edu.ljj.flink.xiaohanying.Utils.PathPointData> pointMap = new ConcurrentHashMap<>();
    static boolean firstEnter = true;
    static Map<Long, PathPoint> tempMap = new ConcurrentHashMap<>();
    private static String pathTimeStamp = "";
    private static long pathTime = 0;
    private static long temp = 0;
    static List<whu.edu.ljj.flink.xiaohanying.Utils.Location> roadAKDataList;
    static List<whu.edu.ljj.flink.xiaohanying.Utils.Location> roadBKDataList;
    static List<whu.edu.ljj.flink.xiaohanying.Utils.Location> roadCKDataList;
    static List<whu.edu.ljj.flink.xiaohanying.Utils.Location> roadDKDataList;
    private static final long BRIDGE_START = 1050000;
    private static final long BRIDGE_END = 1055000;
    // 添加清理相关常量
    private static final long CLEANUP_INTERVAL = 60000; // 清理间隔1分钟
    private static final long EXPIRATION_TIME = 10 * 60 * 1000; // 10分钟超时

    static {
        try {
            roadAKDataList = JsonReader.readJsonFile("AK_locations.json");
            roadBKDataList = JsonReader.readJsonFile("BK_locations.json");
            roadCKDataList = JsonReader.readJsonFile("CK_locations.json");
            roadDKDataList = JsonReader.readJsonFile("DK_locations.json");

            // 启动清理线程
            startCleanupThread();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000); // 超时时间 60s
//        env.getCheckpointConfig().setCheckpointStorage("file:///d:/integrate");
        env.setParallelism(11);
        env.getConfig().setAutoWatermarkInterval(100); // 每 100ms 生成一次水位线

        List<String> topics = Arrays.asList(
                "fiberData1",
                "fiberData2",
                "fiberData3",
                "fiberData4",
                "fiberData5",
                "fiberData6",
                "fiberData7",
                "fiberData8",
                "fiberData9",
                "fiberData10",
                "fiberData11"
            );

        KafkaSource<String> fiberDataTestSource = KafkaSource.<String>builder()
                .setTopics(topics.get(0))
                .setBootstrapServers("10.48.53.82:9092")
                .setGroupId("fiberDataTest-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setProperty("auto.offset.commit", "true")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<PathTData> unionStream = env.fromSource(fiberDataTestSource,
                WatermarkStrategy.noWatermarks(),
                "fiberDataTest Source " + 1)
                .setParallelism(1)
                .map(trajejson -> {
                    PathTData pathData = JSON.parseObject(trajejson, PathTData.class);
                    pathData.setTime(convertToTimestampMillis(pathData.getTimeStamp()));
                    pathData.setSegId(1);
                    return pathData;
                }).setParallelism(1);
//        unionStream.map(value -> {
//                System.out.println("unionStream里的数据为："+value);
//                return value;
//            });

        // 按照topics顺序创建 Kafka 数据源
        for(int i = 1; i < topics.size(); i++) {
            KafkaSource<String> source = KafkaSource.<String>builder()
                    .setTopics(topics.get(i))
                    .setBootstrapServers("10.48.53.82:9092")
                    .setGroupId("fiberDataTest-group")
                    .setStartingOffsets(OffsetsInitializer.latest())
                    .setProperty("auto.offset.commit", "true")
                    .setValueOnlyDeserializer(new SimpleStringSchema())
                    .build();
            // 使用局部final变量，以保证lambda表达式可以正常使用
            final int segmentId = i + 1;
            DataStream<PathTData> stream = env.fromSource(source,
                            WatermarkStrategy.noWatermarks(),
                            "fiberDataTest Source " + segmentId)
                    .setParallelism(1)
                    .map(trajejson -> {
                        PathTData pathData = JSON.parseObject(trajejson, PathTData.class);
                        pathData.setTime(convertToTimestampMillis(pathData.getTimeStamp()));
                        if (segmentId == 5)
                            pathData.setSegId(segmentId * 5);
                        else if(segmentId == 7)
                            pathData.setSegId(segmentId * 7 - 2);
                        else if(segmentId == 8)
                            pathData.setSegId(segmentId * 3);
                        else
                            pathData.setSegId(segmentId);
                        return pathData;
                    }).setParallelism(1);

            unionStream = unionStream.union(stream);
        }

        // 这里其实给每个点加上经纬度和时间以及车牌匹配槽在上一个算子就可以，但是为了模拟开销，单独设一个算子
        KeyedStream<PathTData, Integer> pathDataKeyedStream = unionStream.keyBy(PathTData::getSegId);

        SingleOutputStreamOperator<PathTData> mergedStream = pathDataKeyedStream.process(new KeyedProcessFunction<Integer, PathTData, PathTData>() {
            private transient List<TrafficEventUtils.MileageConverter> mConverterList;

            @Override
            public void open(Configuration parameters) throws Exception {
                mConverterList = new ArrayList<>();
                List<String> convertPathList = Arrays.asList(
                        "sx_json.json",
                        "xx_json.json"
                );
                // 初始化状态
                try {
                    for(String convertPath : convertPathList) {
                        TrafficEventUtils.MileageConverter mileageConverter = new TrafficEventUtils.MileageConverter(convertPath);
                        mConverterList.add(mileageConverter);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void processElement(PathTData value, KeyedProcessFunction<Integer, PathTData, PathTData>.Context ctx, Collector<PathTData> out) throws Exception {
                String timestamp = value.getTimeStamp();
                for(PathPoint ppoint : value.getPathList()) {
                    ppoint.setTimeStamp(timestamp);
                    TrafficEventUtils.StakeInfo stakeInfo = mConverterList.get(ppoint.getDirection() - 1).findCoordinate(ppoint.getMileage());
                    if(stakeInfo != null) {
                        ppoint.setLatitude(stakeInfo.getLnglat()[1]);
                        ppoint.setLongitude(stakeInfo.getLnglat()[0]);
                    }
                    ppoint.setSimulatedPlateNo(ppoint.getPlateNo());
                    ppoint.setPlateNo("");
                }
                out.collect(value);
            }
        }).setParallelism(11);

//        mergedStream.print();

        // 接入模拟的卡口数据
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setTopics("tollData")
                .setBootstrapServers("10.48.53.82:9092")
                .setGroupId("gantry-group-simulation")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setProperty("auto.offset.commit", "true")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> gantryStringSource = env.fromSource(kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Simulated Gantry Source");

        SingleOutputStreamOperator<GantryData> gantryDataStream = gantryStringSource.process(new ProcessFunction<String, GantryData>() {
            private transient GantryAssignment gantryAssign;

            @Override
            public void open(Configuration parameters) throws Exception {
                // 初始化状态
                try {
                    gantryAssign = new GantryAssignment("jgaGantry.xlsx");
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void processElement(String value, ProcessFunction<String, GantryData>.Context ctx, Collector<GantryData> out) throws Exception {

                // 解析卡口数据
                List<GantryData> gantryDataList = JSON.parseArray(value, GantryData.class);
//                System.out.println(gantryDataList);

                for(GantryData gantryData : gantryDataList) {
//                    System.out.println("运行到了这里");
                    // 补充卡口信息
                    // 好奇怪这里为什么不报错？
                    gantryData.setSegId(gantryAssign.getGantrySegAssign().get(gantryData.getDeviceId()));
                    gantryData.setMileage(gantryAssign.getGantriesByID().get(gantryData.getDeviceId()).getMileage());
                    gantryData.setDirection(gantryAssign.getGantriesByID().get(gantryData.getDeviceId()).getDirection());
//                    System.out.println("\n即将要参与匹配的gantry："+gantryData);
                    out.collect(gantryData);
                }
            }
        });

//        gantryDataStream.print();

        /*
            分key操作
            5.10
             a.这里要跟前面光栅分段的id一致
         */
        KeyedStream<GantryData, Integer> keyedGantryStream = gantryDataStream.keyBy(GantryData::getSegId);

        KeyedStream<PathTData, Integer> keyedBufferedTrajeStream = mergedStream.keyBy(PathTData::getSegId).process(new KeyedProcessFunction<Integer, PathTData, PathTData>() {

            private ListState<PathTData> bufferState;
            private final StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.seconds(5))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build();

            @Override
            public void open(Configuration parameters) {
//                System.out.println("已经进来了");
                ListStateDescriptor<PathTData> bufferDescriptor =
                        new ListStateDescriptor<>("bufferState", TypeInformation.of(new TypeHint<PathTData>() {
                        }));
                bufferDescriptor.enableTimeToLive(ttlConfig);
                bufferState = getRuntimeContext().getListState(bufferDescriptor);
            }

            @Override
            public void processElement(PathTData value, KeyedProcessFunction<Integer, PathTData, PathTData>.Context ctx, Collector<PathTData> out) throws Exception {
//                System.out.println("此时传进来的PathTData的时间为：" + value.getTime());
                List<PathTData> bufferData = new ArrayList<>();
                for (PathTData data : bufferState.get())
                    bufferData.add(data);
                if (!bufferData.isEmpty()) {
                    bufferData.sort(Comparator.comparingLong(PathTData::getTime));
                    if (bufferData.get(bufferData.size() - 1).getTime() - bufferData.get(0).getTime() >= 2000) {
                        out.collect(bufferData.get(0));
                        bufferData.remove(0);
                    }
                }
                bufferData.add(value);
                bufferState.update(bufferData);
            }
        }).setParallelism(11).keyBy(PathTData::getSegId);

//        keyedBufferedTrajeStream.map(value -> {
//            System.out.println("此时数据的segId为："+value.getSegId());
//            return null;
//        });

//         为合并后的光栅数据流添加键控
//        KeyedStream<PathTData, Integer> keyedTrajeStream = bufferedMergedStream.keyBy(traje -> traje.getSegId());

        SingleOutputStreamOperator<PathTData> pathTDataStream = keyedBufferedTrajeStream.connect(keyedGantryStream)
                .process(new whu.edu.moniData.shenZhou.TrajectoryEnricherLocalSV5P3()).setParallelism(11).assignTimestampsAndWatermarks(WatermarkStrategy.<PathTData>forBoundedOutOfOrderness(Duration.ofMillis(300))
                        .withTimestampAssigner(new SerializableTimestampAssigner<PathTData>() {
                                                   @Override
                                                   public long extractTimestamp(PathTData pathData, long recordTimestamp) {
                                                       return pathData.getTime();
                                                   }
                                               }
                        ).withIdleness(Duration.ofSeconds(10)));// 超过10s不更新则标记为空闲分区;

        // 合并11段路段
        SingleOutputStreamOperator<PathTData> mergedPathTDataStream = pathTDataStream.keyBy(PathTData::getSegId).windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.milliseconds(200))) // 200ms滚动窗口
                .aggregate(new AggregateFunction<PathTData, PathTData, PathTData>() {

                    @Override
                    public PathTData createAccumulator() {
                        List<PathPoint> ppointList = new ArrayList<>();
                        // 返回结果中不能有SegId，这里设置为null
                        return new PathTData(0, 0L, "", null, ppointList);
                    }

                    @Override
                    public PathTData add(PathTData value, PathTData accumulator) {
                        if (accumulator.getTime() == 0L)
                            accumulator.setTime(value.getTime());
                        if (Objects.equals(accumulator.getTimeStamp(), ""))
                            accumulator.setTimeStamp(value.getTimeStamp());
                        accumulator.getPathList().addAll(value.getPathList());

                        accumulator.setPathNum(accumulator.getPathNum() + value.getPathNum());
                        return accumulator;
                    }

                    @Override
                    public PathTData getResult(PathTData accumulator) {
                        return accumulator;
                    }

                    @Override
                    public PathTData merge(PathTData a, PathTData b) {
                        System.out.println("出现错误：聚合窗口中莫名的合并函数merge调用");
                        return null;
                    }
                })
                .setParallelism(1);
        mergedPathTDataStream = mergedPathTDataStream.map(pathData -> {
            List<PathPoint> points = pathData.getPathList();
            List<String>l=new ArrayList<>();
            // 输出每个点的 simulatedPlateNo
            for (PathPoint point : points) {
                l.add(point.getPlateNo());
            }

            // 输出点的总数
//            System.out.println(l+"   总点数：" + points.size());

            return pathData;
        }).setParallelism(1);

        SingleOutputStreamOperator<PathTData> endPathTDataStream = mergedPathTDataStream.flatMap(new FlatMapFunction<PathTData, PathTData>() {
            @Override
            public void flatMap(PathTData pathTData, Collector<PathTData> collector) throws Exception {
                long st1=System.currentTimeMillis();
                List<PathPoint> list = new ArrayList<>();
                PathTData pathTData1 = initResPathTDate(pathTData);
                String ts = pathTData.getTimeStamp();
                System.out.println("ts:"+ts);
                temp = initCurrentTime(ts);

                if (!pathTData.getPathList().isEmpty()) {
                    if (firstEnter) {
                        list = firstEnterInitializePointMap(pathTData);
                        pathTData1.setPathList(list);
                    } else {
                        putNowDataIntoTempMap(pathTData, ts);

                        // 当前的所有数据直接加入
                        for (Map.Entry<Long, PathPoint> entry : tempMap.entrySet()) {
                            PathPoint p = entry.getValue();
                            if (p.getStakeId() != null && p.getTimeStamp() != null) {
                                list.add(p);
                                pointMap.put(p.getId(), PPToPD(p));
                                double mileage = p.getMileage();
//                                    if(mileage >= BRIDGE_START && mileage <= BRIDGE_END){
//                                        System.out.println(p.getMileage()+" "+p.getPlateNo());
//                                    }
                            }
                        }

                        // 如果里程越界，会被移除
                        for (Map.Entry<Long, whu.edu.ljj.flink.xiaohanying.Utils.PathPointData> entry : pointMap.entrySet()) {
                            if (tempMap.get(entry.getKey()) == null) {
                                // 检查是否过期
                                long currentTime = System.currentTimeMillis();
                                if (currentTime - entry.getValue().getLastUpdateTime() > EXPIRATION_TIME) {
                                    pointMap.remove(entry.getKey());
                                    continue;
                                }
//                                    System.out.println("进入预测，id："+entry.getKey());

                                whu.edu.ljj.flink.xiaohanying.Utils.PathPointData pdInPointMap = predictNextMixed(entry.getKey(), pathTData.getTimeStamp());
                                if (pdInPointMap != null) {
//                                    System.out.println("预测结果："+pdInPointMap.getPlateNo()+"   里程:"+pdInPointMap.getMileage());
                                    list.add(PDToPP(pdInPointMap));
                                    pointMap.put(pdInPointMap.getId(), pdInPointMap);
                                    double mileage = pdInPointMap.getMileage();
//                                        if(mileage >= BRIDGE_START && mileage <= BRIDGE_END){
//                                            System.out.println(pdInPointMap.getMileage()+" "+pdInPointMap.getPlateNo());
//                                        }
                                }
                            }
                        }
                    }
                    pathTData1.setPathList(list);
                    collector.collect(pathTData1);
                    tempMap.clear();
                    long st=System.currentTimeMillis();
                    System.out.println("time used:"+(st-st1));
                }
            }
        }).setParallelism(1);

        writeIntoKafka(endPathTDataStream);
//                .map(v -> {
//                    System.out.println(v.getTimeStamp()+"-此200ms内收集到的PathPoint总数："+v.getPathNum());
//                    System.out.println();
//                    return null;
//                }).setParallelism(1);

        // 在作业主流程中设置RedisSink
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder()
                .setHost("100.65.38.141")
                .setPort(6380)
                .setPassword("whdx123cgz666")  // 设置 Redis 密码
                .build();

        // 车辆状态更新Sink
        RedisSink<Tuple2<String, String>> vehicleStateSink = new RedisSink<>(
                conf, new RedisMapper<Tuple2<String, String>>() {
            @Override
            public RedisCommandDescription getCommandDescription() {
                // 设置2h过期
                return new RedisCommandDescription(RedisCommand.SETEX, 2*60*60);
            }

            @Override
            public String getKeyFromData(Tuple2<String, String> stringStringTuple2) {
                return stringStringTuple2.f0;
            }

            @Override
            public String getValueFromData(Tuple2<String, String> stringStringTuple2) {
                return stringStringTuple2.f1;
            }
        });

        // 精细匹配Sink
        RedisSink<Tuple2<String, String>> fineMatchSink = new RedisSink<>(
                conf, new RedisMapper<Tuple2<String, String>>() {
            @Override
            public RedisCommandDescription getCommandDescription() {
                // 设置12h过期
                return new RedisCommandDescription(RedisCommand.SETEX, 12*60*60);
            }

            @Override
            public String getKeyFromData(Tuple2<String, String> stringStringTuple2) {
                return stringStringTuple2.f0;
            }

            @Override
            public String getValueFromData(Tuple2<String, String> stringStringTuple2) {
                return stringStringTuple2.f1;
            }
        });

        pathTDataStream.getSideOutput(VS_TAG).addSink(vehicleStateSink);
        pathTDataStream.getSideOutput(FM_TAG).addSink(fineMatchSink);

        pathTDataStream.getSideOutput(VS_TAG).map(value -> {
//            System.out.println(System.currentTimeMillis()+", "+value.f0+", vs测流输出");
            return null;
        });
        pathTDataStream.getSideOutput(FM_TAG).map(value -> {
//            System.out.println(System.currentTimeMillis()+", "+value.f0+", fm测流输出");
            return null;
        });

        env.execute("Plate Matching Flink Job");
    }

    // 写入测试用的DynamicKafkaSerializer
    public static class DynamicKafkaSerializer implements KafkaRecordSerializationSchema<PathTData> {
        @Override
        public ProducerRecord<byte[], byte[]> serialize(PathTData element, KafkaSinkContext context, Long timestamp) {
            // 解析 JSON 获取 segId（假设 element 是 JSON 字符串）
            int segId = element.getSegId();
            if(segId == 25)
                segId = 5;
            else if(segId == 47)
                segId = 7;
            else if(segId == 24)
                segId = 8;
            String targetTopic = "fiberData" + segId; // 动态生成 Topic 名称

            // 序列化 Value
            byte[] value = JSON.toJSONString(element).getBytes(StandardCharsets.UTF_8);

            // 返回 ProducerRecord（Key 为 null，Value 为原始 JSON 字符串）
            return new ProducerRecord<>(targetTopic, null, value);
        }
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Setter
    @Getter
    public static class MergedSegAccumulator {
        int sum;
        List<Integer> segIdList;
        Long timeStamp;
    }
    // 清理线程
    private static void startCleanupThread() {
        Thread cleanupThread = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(CLEANUP_INTERVAL);
                    cleanExpiredVehicles();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });
        cleanupThread.setDaemon(true);
        cleanupThread.start();
    }

    // 清理过期车辆
    private static void cleanExpiredVehicles() {
        long now = System.currentTimeMillis();
        Iterator<Map.Entry<Long, whu.edu.ljj.flink.xiaohanying.Utils.PathPointData>> iterator = pointMap.entrySet().iterator();

        while (iterator.hasNext()) {
            Map.Entry<Long, whu.edu.ljj.flink.xiaohanying.Utils.PathPointData> entry = iterator.next();
            whu.edu.ljj.flink.xiaohanying.Utils.PathPointData data = entry.getValue();

            // 检查是否超过10分钟未更新
            if (now - data.getLastUpdateTime() > EXPIRATION_TIME) {
                iterator.remove();
//                System.out.println("清理过期车辆: " + data.getId() + " | 车牌: " + data.getPlateNo());
            }
        }
    }
    private static whu.edu.ljj.flink.xiaohanying.Utils.PathPointData PPToPDAndinitLastRecAndWindow(PathPoint pp) {
        whu.edu.ljj.flink.xiaohanying.Utils.PathPointData pd = PPToPD(pp);
        ConcurrentLinkedDeque<Double> c = new ConcurrentLinkedDeque<>();
        c.add(pp.getSpeed());
        pd.setSpeedWindow(c);
        pd.setLastReceivedTime(0);
        pd.setLastUpdateTime(System.currentTimeMillis()); // 设置初始更新时间
        return pd;
    }

    private static whu.edu.ljj.flink.xiaohanying.Utils.PathPointData predictNextMixed(long keyInPointMap, String timestamp) {
        whu.edu.ljj.flink.xiaohanying.Utils.PathPointData pdInPointMap = pointMap.get(keyInPointMap);
        if (pdInPointMap == null) return null;

        // 检查是否超过10分钟未更新
        long currentTime = System.currentTimeMillis();
        if (currentTime - pdInPointMap.getLastUpdateTime() > EXPIRATION_TIME) {
            pointMap.remove(keyInPointMap);
            tempMap.remove(keyInPointMap);
            return null;
        }

        Pair<ConcurrentLinkedDeque<Double>, Double> a1 = predictSpeedWindow(pdInPointMap);
        double[] a2 = predictNewMileage(pdInPointMap, a1.getValue());

        if (a2[0] < 1016020 || a2[1] > 1173790) {
            pointMap.remove(keyInPointMap);
            tempMap.remove(keyInPointMap);
            return null;
        }

        Pair<String, double[]> a3 = predictStake(pdInPointMap, a2[0], a2[1]);
        if (a3 == null) {
            return null;
        }

        double carangle = calculateBearing(a3.getValue()[1], a3.getValue()[0], pdInPointMap.getLatitude(), pdInPointMap.getLongitude());
        pdInPointMap.setCarAngle(carangle);
        pdInPointMap.setMileage(a2[0]);
        pdInPointMap.setSpeed(a1.getValue());
        pdInPointMap.setLatitude(a3.getValue()[1]);
        pdInPointMap.setLongitude(a3.getValue()[0]);
        pdInPointMap.setSpeedWindow(a1.getKey());
        pdInPointMap.setStakeId(a3.getKey());
        pdInPointMap.setTimeStamp(timestamp);
        pdInPointMap.setLastReceivedTime(1);
        pdInPointMap.setLastUpdateTime(System.currentTimeMillis()); // 更新最后更新时间
        String a=pdInPointMap.getPlateNo();
        if(a.isEmpty()){
            pdInPointMap.setPlateNo("空      预测值");
        }else{
            if (!a.endsWith("值")) {
                pdInPointMap.setPlateNo(a+ " " + "预测值");
            } else {
                pdInPointMap.setPlateNo(a.substring(0, 7) + " " + "预测值");

            }
}


        return pdInPointMap;
    }

    public static double[] predictNewMileage(whu.edu.ljj.flink.xiaohanying.Utils.PathPointData data, double speed) {
        double[] d = {0, 0};
        d[1] = myTools.calculateDistance(speed, 200);
        if (data.getDirection() == 1) {
            d[0] = data.getMileage() + d[1]; // 更新里程点
        } else {
            d[0] = data.getMileage() - d[1]; // 更新里程点
        }
        return d;
    }

    private static void putNowDataIntoTempMap(PathTData pathTData, String time) {
        List<PathPoint> p = pathTData.getPathList();
        for (PathPoint m : p) {
            m.setTimeStamp(time);
            tempMap.put(m.getId(), m);

            // 更新pointMap中车辆的最后更新时间
            whu.edu.ljj.flink.xiaohanying.Utils.PathPointData existing = pointMap.get(m.getId());
            if (existing != null) {
                existing.setLastUpdateTime(System.currentTimeMillis());
            }
        }
    }

    private static List<PathPoint> firstEnterInitializePointMap(PathTData pathTData) {
        List<PathPoint> p = pathTData.getPathList();
        for (PathPoint m : p) {
            whu.edu.ljj.flink.xiaohanying.Utils.PathPointData pp = PPToPD(m);
            pp.setLastReceivedTime(0);
            pp.getSpeedWindow().add(m.getSpeed());
            String ts = pathTData.getTimeStamp();
            pp.setTimeStamp(ts);
            pp.setLastUpdateTime(System.currentTimeMillis()); // 设置初始更新时间
            m.setTimeStamp(ts);
            pointMap.put(m.getId(), pp);
        }
        firstEnter = false;
        return p;
    }

    private static PathPoint PDToPP(whu.edu.ljj.flink.xiaohanying.Utils.PathPointData Point) {
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

    private static whu.edu.ljj.flink.xiaohanying.Utils.PathPointData PPToPD(PathPoint Point) {
        whu.edu.ljj.flink.xiaohanying.Utils.PathPointData pathPoint = new whu.edu.ljj.flink.xiaohanying.Utils.PathPointData();
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
        pathPoint.setLastUpdateTime(System.currentTimeMillis()); // 初始化最后更新时间
        return pathPoint;
    }

    public static long initCurrentTime(String time) {
        if (time == null || time.isEmpty()) {
            // 返回当前时间作为默认值
            return System.currentTimeMillis();
        }
        try {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS");
            LocalDateTime localDateTime = LocalDateTime.parse(time, formatter);
            return localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        } catch (Exception e) {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SS");
            LocalDateTime localDateTime = LocalDateTime.parse(time, formatter);
            return localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        }
    }

    public static PathTData initResPathTDate(PathTData pathTData) {
        pathTimeStamp = pathTData.getTimeStamp();
        pathTime = pathTData.getTime();
        PathTData pathTData1 = new PathTData();
        pathTData.setTime(pathTime);
        pathTData.setTimeStamp(pathTimeStamp);
        pathTData.setPathNum(pathTData1.getPathNum());
        return pathTData1;
    }

    public static void writeIntoKafka(SingleOutputStreamOperator<PathTData> endPathTDataStream) {
        Properties producerProps = new Properties();
        producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.48.53.82:9092");
        producerProps.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "10485760");
        producerProps.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");
        producerProps.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "1048576");

        final int MAX_UNCOMPRESSED_SIZE = 5 * 1024 * 1024;

        DataStream<String> jsonStream = endPathTDataStream
                .flatMap(new RichFlatMapFunction<PathTData, String>() {
                    private transient Gson gson;

                    @Override
                    public void open(Configuration parameters) {
                        gson = new Gson();
                    }

                    @Override
                    public void flatMap(PathTData data, Collector<String> out) {
                        try {
                            long baseTime = data.getTime();
                            String baseTimestamp = data.getTimeStamp();
                            int totalPoints = data.getPathList().size();

                            int chunkSize = totalPoints;
                            List<List<PathPoint>> chunks = partitionData(data.getPathList(), chunkSize);

                            for (int i = 0; i < chunks.size(); i++) {
                                PathTData chunkData = new PathTData();
                                chunkData.setTime(baseTime);
                                chunkData.setTimeStamp(pathTimeStamp);
                                chunkData.setPathNum(chunks.get(i).size());
                                chunkData.setPathList(chunks.get(i));

                                String json = gson.toJson(chunkData);
                                byte[] jsonBytes = json.getBytes(StandardCharsets.UTF_8);

                                if (jsonBytes.length > MAX_UNCOMPRESSED_SIZE) {
                                    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                                         ZstdOutputStream zos = new ZstdOutputStream(bos)) {
                                        zos.write(jsonBytes);
                                        zos.close();
                                        byte[] compressed = bos.toByteArray();
                                        out.collect("ZSTD:" + Base64.getEncoder().encodeToString(compressed));
                                    }
                                } else {
                                    out.collect(json);
                                }
                            }
                        } catch (Exception e) {
                            System.err.println("消息处理出错: " + e.getMessage());
                            e.printStackTrace();
                        }
                    }



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
                .returns(String.class).setParallelism(1);;

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("10.48.53.82:9092")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("completed.pathdata")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setKafkaProducerConfig(producerProps)
                .build();

        jsonStream.sinkTo(sink).setParallelism(1);;
    }
}
