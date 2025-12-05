package whu.edu.moniData.ingest.holyAnalysisJob.redisAndHbase.jtmn;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import whu.edu.ljj.flink.xiaohanying.Utils.PathPoint;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.*;

import static whu.edu.ljj.flink.xiaohanying.Utils.convertToTimestampMillis;

public class hourlyJobWithZaDailyjtmnyuan {
    // 日志记录器
    private static final Logger logger = Logger.getLogger(hourlyJobWithZaDailyjtmnyuan.class.getName());

    // 静态初始化块，设置日志格式
    static {
        try {
            // 设置日志级别
            Logger rootLogger = Logger.getLogger("");
            rootLogger.setLevel(Level.INFO);

            // 移除默认的handlers
            for (Handler handler : rootLogger.getHandlers()) {
                rootLogger.removeHandler(handler);
            }

            // 创建自定义格式器
            SimpleFormatter formatter = new SimpleFormatter() {
                private static final String FORMAT = "[%1$tF %1$tT.%1$tL] [%2$-7s] [%3$s] %4$s%5$s%n";

                @Override
                public synchronized String format(LogRecord lr) {
                    String source;
                    if (lr.getSourceClassName() != null) {
                        source = lr.getSourceClassName();
                        if (lr.getSourceMethodName() != null) {
                            source += "." + lr.getSourceMethodName();
                        }
                    } else {
                        source = lr.getLoggerName();
                    }

                    String message = formatMessage(lr);
                    String throwable = "";
                    if (lr.getThrown() != null) {
                        StringWriter sw = new StringWriter();
                        PrintWriter pw = new PrintWriter(sw);
                        pw.println();
                        lr.getThrown().printStackTrace(pw);
                        pw.close();
                        throwable = sw.toString();
                    }

                    return String.format(FORMAT,
                            new Date(lr.getMillis()),
                            lr.getLevel().getLocalizedName(),
                            source,
                            message,
                            throwable);
                }
            };

            // 控制台处理器
            ConsoleHandler consoleHandler = new ConsoleHandler();
            consoleHandler.setLevel(Level.INFO);
            consoleHandler.setFormatter(formatter);
            rootLogger.addHandler(consoleHandler);

        } catch (Exception e) {
            System.err.println("Failed to initialize logger: " + e.getMessage());
        }
    }

    private static final ConcurrentHashMap<String, Object> tableCreationLocks = new ConcurrentHashMap<>();
    private static final ReentrantLock tableLock = new ReentrantLock();

    // 表名常量
    private static final String TABLE_NAME_TOTAL = "jtmnstats";
    private static final String TABLE_NAME_DETAIL = "jtmnsection";
    private static final String TABLE_NAME_RAMP = "jtmnrampstats";
    private static final String TABLE_NAME_DAILY_TOTAL = "jtmndailystats";
    private static final String TABLE_NAME_DAILY_DETAIL = "jtmndailysection";
    private static final String COLUMN_FAMILY = "stats";

    // 路段定义
    private static final List<RoadSection> ROAD_SECTIONS = Arrays.asList(
            new RoadSection("鄂北-大新段", 1016020, 1030448),
            new RoadSection("大新-大悟段", 1030448, 1043400),
            new RoadSection("大悟-阳平段", 1043400, 1058300),
            new RoadSection("阳平-大悟南枢纽段", 1058300, 1062700),
            new RoadSection("大悟南枢纽-小河段", 1062700, 1075200),
            new RoadSection("小河-孝昌段", 1075200, 1092242),
            new RoadSection("孝昌-桃花驿站段", 1092242, 1110002),
            new RoadSection("桃花驿-孝南枢纽段", 1110002, 1115583),
            new RoadSection("孝南枢纽-孝感东段", 1115583, 1122200),
            new RoadSection("孝感东-府河段", 1122200, 1129200),
            new RoadSection("府河-灯塔枢纽段", 1129200, 1140371),
            new RoadSection("灯塔枢纽-东西湖枢纽段", 1140371, 1148571),
            new RoadSection("东西湖枢纽-武汉北段", 1148571, 1153992),
            new RoadSection("武汉北-蔡甸枢纽段", 1153992, 1163000),
            new RoadSection("蔡甸枢纽-天鹅湖段", 1163000, 1168100),
            new RoadSection("天鹅湖-武汉西枢纽段", 1168100, 1173535)
    );

    // 判断客车类型的方法
    private static boolean isBus(int vt) {
        return vt == 1 || vt == 3 || vt == 7 || vt == 15;
    }

    // 判断货车类型的方法
    private static boolean isTrack(int vt) {
        return vt == 2 || vt == 10 || vt == 8 || vt == 11 || vt == 170 || vt == 171 || vt == 172 ||
                vt == 173 || vt == 174 || vt == 175 || vt == 176 || vt == 177;
    }

    // 匝道车辆类型判断方法
    private static int getVehicleClass(int originalType) {
        if ((originalType >= 1 && originalType <= 4) || originalType == 7 || (originalType >= 12 && originalType <= 16)) {
            return 0; // 客车
        }
        if (originalType == 8 || originalType == 10 || originalType == 11 ||
                (originalType >= 170 && originalType <= 177)) {
            return 1; // 货车
        }
        return -1;
    }

    // 根据桩号获取路段起始桩号
    private static String getStakeMarkByMileage(double mileage) {
        int mileageInt = (int) mileage;
        for (RoadSection section : ROAD_SECTIONS) {
            if (mileageInt >= section.startMileage && mileageInt < section.endMileage) {
                // 将起始桩号转换为桩号标记，例如1016020 -> K1016
                int stakeKm = section.startMileage / 1000;
                return "K" + stakeKm;
            }
        }
        return "未知桩号";
    }

    public static void main(String[] args) throws Exception {
        // ==================== 程序启动信息 ====================
        logger.info("=================================================================");
        logger.info("交通数据分析任务 - 开始执行");
        logger.info("启动时间: " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date()));
        logger.info("运行命令: java -cp /home/ljj/flinkTest-1.0-SNAPSHOT.jar " +
                hourlyJobWithZaDailyjtmnyuan.class.getName());
        logger.info("=================================================================");

        // 打印程序参数（如果有）
        if (args.length > 0) {
            logger.info("程序参数:");
            for (int i = 0; i < args.length; i++) {
                logger.info("  args[" + i + "] = " + args[i]);
            }
        } else {
            logger.info("无程序参数");
        }

        logger.info("创建Flink执行环境...");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        logger.info("Flink执行环境创建成功，并行度: " + env.getParallelism());

        // ==================== 主路数据处理 ====================
        logger.info("开始配置主路数据处理管道...");
        // Kafka配置 - 主路数据
        String brokers = "10.48.53.82:9092";
        String groupId = "hourly-traffic-group";
        List<String> mainRoadTopics = Arrays.asList("jtkj.jga.path");
        logger.info("Kafka配置 - Brokers: " + brokers + ", Group: " + groupId + ", Topics: " + mainRoadTopics);

        // 创建Kafka源 - 主路数据
        KafkaSource<String> mainRoadKafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(mainRoadTopics)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        logger.info("主路Kafka源创建成功");

        // 主路数据流
        DataStream<String> mainRoadSourceStream = env.fromSource(
                mainRoadKafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Main Road Kafka Source"
        );
        logger.info("主路数据流创建成功");

        // 解析JSON为PathPoint对象 - 主路数据
        SingleOutputStreamOperator<PathPoint> mainRoadPathPointStream = mainRoadSourceStream
                .flatMap(new FlatMapFunction<String, PathPoint>() {
                    private long recordCount = 0;
                    private long pointCount = 0;
                    private long lastLogTime = System.currentTimeMillis();

                    @Override
                    public void flatMap(String value, Collector<PathPoint> out) {
                        try {
                            recordCount++;

                            JSONObject json = JSON.parseObject(value);
                            String timestamp = json.getString("timeStamp");
                            JSONArray pathList = json.getJSONArray("pathList");

                            pointCount += pathList.size();

                            long currentTime = System.currentTimeMillis();
                            // 每1000条记录或每10秒输出一次进度
                            if (recordCount % 1000 == 0 || currentTime - lastLogTime > 10000) {
                                logger.info(String.format("主路数据解析进度 - 记录数: %,d, 路径点数: %,d, 速率: %.2f 条/秒",
                                        recordCount, pointCount,
                                        recordCount / ((currentTime - lastLogTime + 1) / 1000.0)));
                                lastLogTime = currentTime;
                            }

                            for (int i = 0; i < pathList.size(); i++) {
                                PathPoint point = pathList.getObject(i, PathPoint.class);
                                point.setTimeStamp(timestamp);
                                if(point.getVehicleType()==null)point.setVehicleType(point.getOriginalType());
                                if(point.getOriginalType()==null)point.setOriginalType(point.getVehicleType());
                                point.setPlateNo("默A"+point.getId());
                                out.collect(point);
                            }
                        } catch (Exception e) {
                            logger.log(Level.SEVERE, "解析主路JSON数据时发生错误: " + e.getMessage(), e);
                        }
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<PathPoint>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((event, recordTimestamp) ->
                                        convertToTimestampMillis(event.getTimeStamp()))
                )
                .name("MainRoadPathPointStream");
        logger.info("主路数据解析管道配置完成");

        // ==================== 匝道数据处理 ====================
        logger.info("开始配置匝道数据处理管道...");
        // Kafka配置 - 匝道数据
        String rampGroupId = "ramp-traffic-group1";
        List<String> rampTopics = Arrays.asList("MergedRampPathData");
        logger.info("匝道Kafka配置 - Topics: " + rampTopics);

        // 创建Kafka源 - 匝道数据
        KafkaSource<String> rampKafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(rampTopics)
                .setGroupId(rampGroupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        logger.info("匝道Kafka源创建成功");

        // 匝道数据流
        DataStream<String> rampSourceStream = env.fromSource(
                rampKafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Ramp Kafka Source"
        );
        logger.info("匝道数据流创建成功");

        // 解析JSON为PathPoint对象 - 匝道数据
        SingleOutputStreamOperator<PathPoint> rampPathPointStream = rampSourceStream
                .flatMap(new FlatMapFunction<String, PathPoint>() {
                    private long recordCount = 0;
                    private long pointCount = 0;

                    @Override
                    public void flatMap(String value, Collector<PathPoint> out) {
                        try {
                            recordCount++;

                            JSONObject json = JSON.parseObject(value);
                            String timestamp = json.getString("timeStamp");
                            JSONArray pathList = json.getJSONArray("pathList");

                            pointCount += pathList.size();

                            if (recordCount % 500 == 0) {
                                logger.info(String.format("匝道数据解析进度 - 记录数: %,d, 路径点数: %,d",
                                        recordCount, pointCount));
                            }

                            for (int i = 0; i < pathList.size(); i++) {
                                PathPoint point = pathList.getObject(i, PathPoint.class);
                                point.setTimeStamp(timestamp);
                                out.collect(point);
                            }
                        } catch (Exception e) {
                            logger.log(Level.SEVERE, "解析匝道JSON数据时发生错误: " + e.getMessage(), e);
                        }
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<PathPoint>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((event, recordTimestamp) ->
                                        convertToTimestampMillis(event.getTimeStamp()))
                )
                .name("RampPathPointStream");
        logger.info("匝道数据解析管道配置完成");

        // ==================== 主路交通量统计（按小时和方向）====================
        logger.info("开始配置主路总交通量统计...");
        DataStream<Tuple3<String, Integer, Integer>> totalTrafficStream = mainRoadPathPointStream
                .flatMap(new FlatMapFunction<PathPoint, Tuple3<String, Long, Integer>>() {
                    @Override
                    public void flatMap(PathPoint point, Collector<Tuple3<String, Long, Integer>> out) {
                        if (point.getDirection() == 1 || point.getDirection() == 2) {
                            long eventTime = convertToTimestampMillis(point.getTimeStamp());
                            String hourKey = new SimpleDateFormat("yyyyMMddHH").format(eventTime);
                            out.collect(new Tuple3<>(hourKey, point.getId(), point.getDirection()));
                        }
                    }
                })
                .keyBy(t -> t.f0)  // 按小时分组
                .window(TumblingEventTimeWindows.of(Time.hours(1))) // 1小时滚动窗口
                .aggregate(new TotalTrafficAggregator())
                .map(new MapFunction<Tuple3<String, Integer, Integer>, Tuple3<String, Integer, Integer>>() {
                    @Override
                    public Tuple3<String, Integer, Integer> map(Tuple3<String, Integer, Integer> tuple) throws Exception {
                        logger.info(String.format("[小时总交通量] 时间: %s, 上行车辆: %,d, 下行车辆: %,d",
                                tuple.f0, tuple.f1, tuple.f2));
                        return tuple;
                    }
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT, Types.INT))
                .name("TotalTrafficStream");

        totalTrafficStream.print("Total Traffic");
        logger.info("主路总交通量统计管道配置完成");

        // 写入总交通量HBase表
        totalTrafficStream.addSink(new TotalHBaseTrafficSink())
                .name("TotalHBaseSink");
        logger.info("总交通量HBase Sink已配置");

        // ==================== 主路详细交通量统计（按小时、路段、方向和类型）====================
        logger.info("开始配置主路详细交通量统计...");
        DataStream<Tuple6<String, String, Integer, Integer, Integer, Integer>> detailedTrafficStream = mainRoadPathPointStream
                .flatMap(new FlatMapFunction<PathPoint, Tuple6<String, String, Integer, Long, Integer, Integer>>() {
                    @Override
                    public void flatMap(PathPoint point, Collector<Tuple6<String, String, Integer, Long, Integer, Integer>> out) {
                        if (point.getDirection() == 1 || point.getDirection() == 2) {
                            long eventTime = convertToTimestampMillis(point.getTimeStamp());
                            String hourKey = new SimpleDateFormat("yyyyMMddHH").format(eventTime);

                            // 根据桩号获取路段起始桩号
                            String stakeMark = getStakeMarkByMileage(point.getMileage());

                            // 判断车辆类型
                            int vehicleType = point.getVehicleType();
                            int isBus = isBus(vehicleType) ? 1 : 0;
                            int isTrack = isTrack(vehicleType) ? 1 : 0;

                            out.collect(new Tuple6<>(hourKey, stakeMark, point.getDirection(), point.getId(), isBus, isTrack));
                        }
                    }
                })
                .keyBy(t -> t.f0 + "_" + t.f1 + "_" + t.f2)  // 按小时+桩号+方向分组
                .window(TumblingEventTimeWindows.of(Time.hours(1))) // 1小时滚动窗口
                .aggregate(new DetailedTrafficAggregator())
                .map(new MapFunction<Tuple6<String, String, Integer, Integer, Integer, Integer>,
                        Tuple6<String, String, Integer, Integer, Integer, Integer>>() {
                    @Override
                    public Tuple6<String, String, Integer, Integer, Integer, Integer> map(
                            Tuple6<String, String, Integer, Integer, Integer, Integer> tuple) throws Exception {
                        logger.info(String.format("[详细交通量] 时间: %s, 桩号: %s, 方向: %d, 客车: %,d, 货车: %,d, 其他: %,d",
                                tuple.f0, tuple.f1, tuple.f2, tuple.f3, tuple.f4, tuple.f5));
                        return tuple;
                    }
                })
                .returns(Types.TUPLE(Types.STRING, Types.STRING, Types.INT, Types.INT, Types.INT, Types.INT))
                .name("DetailedTrafficStream");
        logger.info("主路详细交通量统计管道配置完成");

        // 写入详细交通量HBase表
        detailedTrafficStream.addSink(new DetailedHBaseTrafficSink())
                .name("DetailedHBaseSink");
        logger.info("详细交通量HBase Sink已配置");

        // ==================== 匝道交通量统计 ====================
        logger.info("开始配置匝道交通量统计...");
        DataStream<Tuple7<String, String, Integer, Integer, Integer, Double, Integer>> rampTrafficStream = rampPathPointStream
                .flatMap(new FlatMapFunction<PathPoint, Tuple7<String, String, Long, Integer, Double, Integer, Integer>>() {
                    @Override
                    public void flatMap(PathPoint point, Collector<Tuple7<String, String, Long, Integer, Double, Integer, Integer>> out) {
                        // 检查是否为匝道数据
                        if (point.getStakeId() != null && point.getStakeId().contains("-")) {
                            String[] parts = point.getStakeId().split("-");
                            if (parts.length >= 2) {
                                // 提取匝道编号 (CK0+199 -> C)
                                String rampCode = parts[1].substring(0, 1);
                                if (rampCode.matches("[A-D]")) { // 只处理A,B,C,D四种匝道
                                    long eventTime = convertToTimestampMillis(point.getTimeStamp());
                                    String hourKey = new SimpleDateFormat("yyyyMMddHH").format(eventTime);

                                    // 判断车辆类型
                                    int vehicleClass = getVehicleClass(point.getOriginalType());
                                    int isBus = (vehicleClass == 0) ? 1 : 0;
                                    int isTrack = (vehicleClass == 1) ? 1 : 0;

                                    out.collect(new Tuple7<>(hourKey, rampCode, point.getId(), isBus, point.getSpeed(), isTrack, 1));
                                }
                            }
                        }
                    }
                })
                .keyBy(t -> t.f0 + "_" + t.f1)  // 按小时+匝道编号分组
                .window(TumblingEventTimeWindows.of(Time.hours(1))) // 1小时滚动窗口
                .aggregate(new RampTrafficAggregator())
                .map(new MapFunction<Tuple7<String, String, Integer, Integer, Integer, Double, Integer>,
                        Tuple7<String, String, Integer, Integer, Integer, Double, Integer>>() {
                    @Override
                    public Tuple7<String, String, Integer, Integer, Integer, Double, Integer> map(
                            Tuple7<String, String, Integer, Integer, Integer, Double, Integer> tuple) throws Exception {
                        logger.info(String.format("[匝道交通量] 时间: %s, 匝道: %s, 车辆数: %,d, 客车: %,d, 货车: %,d, 均速: %.2f",
                                tuple.f0, tuple.f1, tuple.f2, tuple.f3, tuple.f4, tuple.f5));
                        return tuple;
                    }
                })
                .returns(Types.TUPLE(Types.STRING, Types.STRING, Types.INT, Types.INT, Types.INT, Types.DOUBLE, Types.INT))
                .name("RampTrafficStream");
        logger.info("匝道交通量统计管道配置完成");

        // 写入匝道交通量HBase表
        rampTrafficStream.addSink(new RampHBaseTrafficSink())
                .name("RampHBaseSink");
        logger.info("匝道交通量HBase Sink已配置");

        // ==================== 每日去重统计（按两小时去重）====================
        logger.info("开始配置每日统计管道...");

        // 每日总交通量统计（按天和方向）
        DataStream<Tuple3<String, Integer, Integer>> dailyTotalTrafficStream = mainRoadPathPointStream
                .flatMap(new FlatMapFunction<PathPoint, Tuple4<String, Long, Integer, Long>>() {
                    @Override
                    public void flatMap(PathPoint point, Collector<Tuple4<String, Long, Integer, Long>> out) {
                        if (point.getDirection() == 1 || point.getDirection() == 2) {
                            long eventTime = convertToTimestampMillis(point.getTimeStamp());
                            String dayKey = new SimpleDateFormat("yyyyMMdd").format(eventTime);

                            out.collect(new Tuple4<>(dayKey, point.getId(), point.getDirection(), eventTime));
                        }
                    }
                })
                .keyBy(t -> t.f0)  // 按天分组
                .window(TumblingEventTimeWindows.of(Time.days(1))) // 1天滚动窗口
                .aggregate(new DailyTotalTrafficAggregator())
                .map(new MapFunction<Tuple3<String, Integer, Integer>, Tuple3<String, Integer, Integer>>() {
                    @Override
                    public Tuple3<String, Integer, Integer> map(Tuple3<String, Integer, Integer> tuple) throws Exception {
                        logger.info(String.format("[每日总交通量] 日期: %s, 上行车辆: %,d, 下行车辆: %,d",
                                tuple.f0, tuple.f1, tuple.f2));
                        return tuple;
                    }
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT, Types.INT))
                .name("DailyTotalTrafficStream");
        logger.info("每日总交通量统计管道配置完成");

        // 写入每日总交通量HBase表
        dailyTotalTrafficStream.addSink(new DailyTotalHBaseTrafficSink())
                .name("DailyTotalHBaseSink");
        logger.info("每日总交通量HBase Sink已配置");

        // 每日详细交通量统计（按天、路段、方向和类型）
        DataStream<Tuple6<String, String, Integer, Integer, Integer, Integer>> dailyDetailedTrafficStream = mainRoadPathPointStream
                .flatMap(new FlatMapFunction<PathPoint, Tuple7<String, String, Integer, Long, Integer, Integer, Long>>() {
                    @Override
                    public void flatMap(PathPoint point, Collector<Tuple7<String, String, Integer, Long, Integer, Integer, Long>> out) {
                        if (point.getDirection() == 1 || point.getDirection() == 2) {
                            long eventTime = convertToTimestampMillis(point.getTimeStamp());
                            String dayKey = new SimpleDateFormat("yyyyMMdd").format(eventTime);

                            // 根据桩号获取路段起始桩号
                            String stakeMark = getStakeMarkByMileage(point.getMileage());

                            // 判断车辆类型
                            int vehicleType = point.getVehicleType();
                            int isBus = isBus(vehicleType) ? 1 : 0;
                            int isTrack = isTrack(vehicleType) ? 1 : 0;

                            out.collect(new Tuple7<>(dayKey, stakeMark, point.getDirection(), point.getId(), isBus, isTrack, eventTime));
                        }
                    }
                })
                .keyBy(t -> t.f0 + "_" + t.f1 + "_" + t.f2)  // 按天+桩号+方向分组
                .window(TumblingEventTimeWindows.of(Time.days(1))) // 1天滚动窗口
                .aggregate(new DailyDetailedTrafficAggregator())
                .map(new MapFunction<Tuple6<String, String, Integer, Integer, Integer, Integer>,
                        Tuple6<String, String, Integer, Integer, Integer, Integer>>() {
                    @Override
                    public Tuple6<String, String, Integer, Integer, Integer, Integer> map(
                            Tuple6<String, String, Integer, Integer, Integer, Integer> tuple) throws Exception {
                        logger.info(String.format("[每日详细交通量] 日期: %s, 桩号: %s, 方向: %d, 客车: %,d, 货车: %,d, 其他: %,d",
                                tuple.f0, tuple.f1, tuple.f2, tuple.f3, tuple.f4, tuple.f5));
                        return tuple;
                    }
                })
                .returns(Types.TUPLE(Types.STRING, Types.STRING, Types.INT, Types.INT, Types.INT, Types.INT))
                .name("DailyDetailedTrafficStream");
        logger.info("每日详细交通量统计管道配置完成");

        // 写入每日详细交通量HBase表
        dailyDetailedTrafficStream.addSink(new DailyDetailedHBaseTrafficSink())
                .name("DailyDetailedHBaseSink");
        logger.info("每日详细交通量HBase Sink已配置");

        // ==================== 作业总结信息 ====================
        logger.info("=================================================================");
        logger.info("所有数据处理管道配置完成！");
        logger.info("已配置的数据处理管道：");
        logger.info("1. 主路总交通量统计 (每小时)");
        logger.info("2. 主路详细交通量统计 (每小时，按桩号和车型)");
        logger.info("3. 匝道交通量统计 (每小时)");
        logger.info("4. 每日总交通量统计 (按天)");
        logger.info("5. 每日详细交通量统计 (按天，按桩号和车型)");
        logger.info("=================================================================");
        logger.info("开始执行Flink作业...");

        // ==================== 执行作业 ====================
        try {
            logger.info("执行Flink作业: Combined Hourly and Daily Traffic Analysis");
            env.execute("Combined Hourly and Daily Traffic Analysis");
            logger.info("=================================================================");
            logger.info("Flink作业执行完成！");
            logger.info("完成时间: " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date()));
            logger.info("=================================================================");
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Flink作业执行过程中发生错误", e);
            logger.info("=================================================================");
            logger.info("作业异常终止");
            logger.info("=================================================================");
            throw e;
        }
    }

    // ==================== 路段定义类 ====================
    private static class RoadSection {
        String sectionName;
        int startMileage;
        int endMileage;

        public RoadSection(String sectionName, int startMileage, int endMileage) {
            this.sectionName = sectionName;
            this.startMileage = startMileage;
            this.endMileage = endMileage;
        }
    }

    // ==================== 总交通量聚合器和累加器 ====================
    private static class TotalTrafficAggregator implements AggregateFunction<
            Tuple3<String, Long, Integer>,
            TotalTrafficAccumulator,
            Tuple3<String, Integer, Integer>> {

        @Override
        public TotalTrafficAccumulator createAccumulator() {
            return new TotalTrafficAccumulator();
        }

        @Override
        public TotalTrafficAccumulator add(Tuple3<String, Long, Integer> value, TotalTrafficAccumulator acc) {
            if (acc.hourKey == null) {
                acc.hourKey = value.f0;
                logger.fine("开始处理时间窗口: " + value.f0);
            }
            acc.addVehicle(value.f1, value.f2);
            return acc;
        }

        @Override
        public Tuple3<String, Integer, Integer> getResult(TotalTrafficAccumulator acc) {
            int totalVehicles = acc.upCount.get() + acc.downCount.get();
            logger.info(String.format("时间窗口 %s 计算完成 - 总车辆数: %,d (上行: %,d, 下行: %,d)",
                    acc.hourKey, totalVehicles, acc.upCount.get(), acc.downCount.get()));
            return Tuple3.of(acc.hourKey, acc.upCount.get(), acc.downCount.get());
        }

        @Override
        public TotalTrafficAccumulator merge(TotalTrafficAccumulator a, TotalTrafficAccumulator b) {
            logger.fine("合并总交通量累加器: " + b.hourKey + " -> " + a.hourKey);
            a.merge(b);
            return a;
        }
    }

    private static class TotalTrafficAccumulator {
        public String hourKey;
        public final Set<Long> vehicleIds = new HashSet<>();
        public final AtomicInteger upCount = new AtomicInteger(0);
        public final AtomicInteger downCount = new AtomicInteger(0);
        private long lastLogTime = System.currentTimeMillis();
        private int addCount = 0;

        public void addVehicle(long vehicleId, int direction) {
            addCount++;

            if (!vehicleIds.contains(vehicleId)) {
                vehicleIds.add(vehicleId);
                if (direction == 1) {
                    upCount.incrementAndGet();
                } else if (direction == 2) {
                    downCount.incrementAndGet();
                }

                // 每10000辆车或每30秒输出一次进度
                long currentTime = System.currentTimeMillis();
                if (vehicleIds.size() % 10000 == 0 || currentTime - lastLogTime > 30000) {
                    logger.fine(String.format("时间窗口 %s 处理进度 - 唯一车辆数: %,d, 总处理数: %,d (上行: %,d, 下行: %,d)",
                            hourKey, vehicleIds.size(), addCount, upCount.get(), downCount.get()));
                    lastLogTime = currentTime;
                }
            }
        }

        public void merge(TotalTrafficAccumulator other) {
            int beforeSize = vehicleIds.size();
            for (Long id : other.vehicleIds) {
                if (!vehicleIds.contains(id)) {
                    vehicleIds.add(id);
                    if (other.upCount.get() > 0) upCount.incrementAndGet();
                    if (other.downCount.get() > 0) downCount.incrementAndGet();
                }
            }
            logger.fine(String.format("合并完成: 唯一车辆数从 %,d 增加到 %,d", beforeSize, vehicleIds.size()));
        }
    }

    // ==================== 详细交通量聚合器和累加器 ====================
    private static class DetailedTrafficAggregator implements AggregateFunction<
            Tuple6<String, String, Integer, Long, Integer, Integer>,
            DetailedTrafficAccumulator,
            Tuple6<String, String, Integer, Integer, Integer, Integer>> {

        @Override
        public DetailedTrafficAccumulator createAccumulator() {
            return new DetailedTrafficAccumulator();
        }

        @Override
        public DetailedTrafficAccumulator add(Tuple6<String, String, Integer, Long, Integer, Integer> value, DetailedTrafficAccumulator acc) {
            if (acc.hourKey == null) {
                acc.hourKey = value.f0;
                acc.stakeMark = value.f1;
                acc.direction = value.f2;
                logger.fine("开始处理详细时间窗口: " + value.f0 + "_" + value.f1 + "_" + value.f2);
            }
            acc.addVehicle(value.f3, value.f4, value.f5);
            return acc;
        }

        @Override
        public Tuple6<String, String, Integer, Integer, Integer, Integer> getResult(DetailedTrafficAccumulator acc) {
            int totalVehicles = acc.busCount.get() + acc.trackCount.get() + acc.otherCount.get();
            logger.info(String.format("详细时间窗口 %s_%s_%d 计算完成 - 总车辆数: %,d (客车: %,d, 货车: %,d, 其他: %,d)",
                    acc.hourKey, acc.stakeMark, acc.direction, totalVehicles,
                    acc.busCount.get(), acc.trackCount.get(), acc.otherCount.get()));
            return Tuple6.of(acc.hourKey, acc.stakeMark, acc.direction,
                    acc.busCount.get(), acc.trackCount.get(), acc.otherCount.get());
        }

        @Override
        public DetailedTrafficAccumulator merge(DetailedTrafficAccumulator a, DetailedTrafficAccumulator b) {
            logger.fine("合并详细交通量累加器");
            a.merge(b);
            return a;
        }
    }

    private static class DetailedTrafficAccumulator {
        public String hourKey;
        public String stakeMark; // 桩号标记，如K1016
        public int direction;
        public final Set<Long> vehicleIds = new HashSet<>();
        public final AtomicInteger busCount = new AtomicInteger(0);
        public final AtomicInteger trackCount = new AtomicInteger(0);
        public final AtomicInteger otherCount = new AtomicInteger(0);
        private int addCount = 0;

        public void addVehicle(long vehicleId, int isBus, int isTrack) {
            addCount++;

            if (!vehicleIds.contains(vehicleId)) {
                vehicleIds.add(vehicleId);
                if (isBus == 1) {
                    busCount.incrementAndGet();
                } else if (isTrack == 1) {
                    trackCount.incrementAndGet();
                } else {
                    otherCount.incrementAndGet();
                }

                if (addCount % 5000 == 0) {
                    logger.fine(String.format("详细窗口 %s_%s_%d 处理进度 - 已处理: %,d, 唯一车辆: %,d",
                            hourKey, stakeMark, direction, addCount, vehicleIds.size()));
                }
            }
        }

        public void merge(DetailedTrafficAccumulator other) {
            for (Long id : other.vehicleIds) {
                if (!vehicleIds.contains(id)) {
                    vehicleIds.add(id);
                    busCount.addAndGet(other.busCount.get());
                    trackCount.addAndGet(other.trackCount.get());
                    otherCount.addAndGet(other.otherCount.get());
                }
            }
        }
    }

    // ==================== 匝道交通量聚合器和累加器 ====================
    private static class RampTrafficAggregator implements AggregateFunction<
            Tuple7<String, String, Long, Integer, Double, Integer, Integer>,
            RampTrafficAccumulator,
            Tuple7<String, String, Integer, Integer, Integer, Double, Integer>> {

        @Override
        public RampTrafficAccumulator createAccumulator() {
            return new RampTrafficAccumulator();
        }

        @Override
        public RampTrafficAccumulator add(Tuple7<String, String, Long, Integer, Double, Integer, Integer> value, RampTrafficAccumulator acc) {
            if (acc.hourKey == null) {
                acc.hourKey = value.f0;
                acc.rampCode = value.f1;
                logger.fine("开始处理匝道时间窗口: " + value.f0 + "_" + value.f1);
            }
            acc.addVehicle(value.f2, value.f3, value.f4, value.f5, value.f6);
            return acc;
        }

        @Override
        public Tuple7<String, String, Integer, Integer, Integer, Double, Integer> getResult(RampTrafficAccumulator acc) {
            double avgSpeed = acc.vehicleCount.get() > 0 ? acc.totalSpeed.get() / acc.vehicleCount.get() : 0.0;
            logger.info(String.format("匝道时间窗口 %s_%s 计算完成 - 唯一车辆: %,d, 总车次: %,d, 客车: %,d, 货车: %,d, 均速: %.2f",
                    acc.hourKey, acc.rampCode, acc.vehicleCount.get(), acc.totalCount.get(),
                    acc.busCount.get(), acc.trackCount.get(), avgSpeed));
            return Tuple7.of(acc.hourKey, acc.rampCode, acc.vehicleCount.get(),
                    acc.busCount.get(), acc.trackCount.get(), avgSpeed, acc.totalCount.get());
        }

        @Override
        public RampTrafficAccumulator merge(RampTrafficAccumulator a, RampTrafficAccumulator b) {
            logger.fine("合并匝道交通量累加器");
            a.merge(b);
            return a;
        }
    }

    private static class RampTrafficAccumulator {
        public String hourKey;
        public String rampCode;
        public final Set<Long> vehicleIds = new HashSet<>();
        public final AtomicInteger busCount = new AtomicInteger(0);
        public final AtomicInteger trackCount = new AtomicInteger(0);
        public final AtomicInteger vehicleCount = new AtomicInteger(0);
        public final AtomicInteger totalCount = new AtomicInteger(0);
        public final AtomicDouble totalSpeed = new AtomicDouble(0.0);
        private int addCount = 0;

        public void addVehicle(long vehicleId, int isBus, double speed, int isTrack, int count) {
            addCount++;
            totalCount.addAndGet(count);
            totalSpeed.addAndGet(speed);

            if (!vehicleIds.contains(vehicleId)) {
                vehicleIds.add(vehicleId);
                vehicleCount.incrementAndGet();
                if (isBus == 1) {
                    busCount.incrementAndGet();
                } else if (isTrack == 1) {
                    trackCount.incrementAndGet();
                }

                if (addCount % 1000 == 0) {
                    logger.fine(String.format("匝道窗口 %s_%s 处理进度 - 已处理: %,d, 唯一车辆: %,d",
                            hourKey, rampCode, addCount, vehicleIds.size()));
                }
            }
        }

        public void merge(RampTrafficAccumulator other) {
            for (Long id : other.vehicleIds) {
                if (!vehicleIds.contains(id)) {
                    vehicleIds.add(id);
                    vehicleCount.addAndGet(1);
                    busCount.addAndGet(other.busCount.get());
                    trackCount.addAndGet(other.trackCount.get());
                }
            }
            totalCount.addAndGet(other.totalCount.get());
            totalSpeed.addAndGet(other.totalSpeed.get());
        }
    }

    // ==================== 每日总交通量聚合器（按两小时去重）====================
    private static class DailyTotalTrafficAggregator implements AggregateFunction<
            Tuple4<String, Long, Integer, Long>,
            DailyTotalTrafficAccumulator,
            Tuple3<String, Integer, Integer>> {

        @Override
        public DailyTotalTrafficAccumulator createAccumulator() {
            return new DailyTotalTrafficAccumulator();
        }

        @Override
        public DailyTotalTrafficAccumulator add(Tuple4<String, Long, Integer, Long> value, DailyTotalTrafficAccumulator acc) {
            if (acc.dayKey == null) {
                acc.dayKey = value.f0;
                logger.fine("开始处理每日总交通量: " + value.f0);
            }
            acc.addVehicle(value.f1, value.f2, value.f3);
            return acc;
        }

        @Override
        public Tuple3<String, Integer, Integer> getResult(DailyTotalTrafficAccumulator acc) {
            int totalVehicles = acc.upCount + acc.downCount;
            logger.info(String.format("每日总交通量 %s 计算完成 - 总车辆数: %,d (上行: %,d, 下行: %,d)",
                    acc.dayKey, totalVehicles, acc.upCount, acc.downCount));
            return Tuple3.of(acc.dayKey, acc.upCount, acc.downCount);
        }

        @Override
        public DailyTotalTrafficAccumulator merge(DailyTotalTrafficAccumulator a, DailyTotalTrafficAccumulator b) {
            logger.fine("合并每日总交通量累加器");
            for (Map.Entry<String, Set<Long>> entry : b.twoHourWindows.entrySet()) {
                String twoHourKey = entry.getKey();
                Set<Long> vehicleSet = a.twoHourWindows.computeIfAbsent(twoHourKey, k -> new HashSet<>());
                for (Long vehicleId : entry.getValue()) {
                    if (!vehicleSet.contains(vehicleId)) {
                        vehicleSet.add(vehicleId);
                    }
                }
            }
            a.recalculateCounts();
            return a;
        }
    }

    private static class DailyTotalTrafficAccumulator {
        public String dayKey;
        // 两小时窗口 -> 车辆ID集合
        public Map<String, Set<Long>> twoHourWindows = new HashMap<>();
        public int upCount = 0;
        public int downCount = 0;
        // 临时存储方向信息
        private Map<Long, Integer> vehicleDirections = new HashMap<>();
        private int addCount = 0;

        public void addVehicle(long vehicleId, int direction, long timestamp) {
            addCount++;

            // 存储车辆方向
            vehicleDirections.put(vehicleId, direction);

            // 将时间戳转换为两小时窗口的起始时间字符串
            String twoHourKey = getTwoHourWindowKey(timestamp);
            Set<Long> vehicleSet = twoHourWindows.computeIfAbsent(twoHourKey, k -> new HashSet<>());

            if (!vehicleSet.contains(vehicleId)) {
                vehicleSet.add(vehicleId);
                if (direction == 1) {
                    upCount++;
                } else if (direction == 2) {
                    downCount++;
                }

                if (addCount % 10000 == 0) {
                    logger.fine(String.format("每日总交通量 %s 处理进度 - 已处理: %,d, 唯一车辆: %,d (上行: %,d, 下行: %,d)",
                            dayKey, addCount, getTotalUniqueVehicles(), upCount, downCount));
                }
            }
        }

        private String getTwoHourWindowKey(long timestamp) {
            // 将时间戳转换为两小时窗口的起始时间
            Date date = new Date(timestamp);
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(date);
            int hour = calendar.get(Calendar.HOUR_OF_DAY);
            hour = (hour / 2) * 2; // 取整到两小时
            calendar.set(Calendar.HOUR_OF_DAY, hour);
            calendar.set(Calendar.MINUTE, 0);
            calendar.set(Calendar.SECOND, 0);
            calendar.set(Calendar.MILLISECOND, 0);
            return new SimpleDateFormat("yyyyMMddHH").format(calendar.getTime());
        }

        public void recalculateCounts() {
            upCount = 0;
            downCount = 0;

            for (Set<Long> vehicleSet : twoHourWindows.values()) {
                for (Long vehicleId : vehicleSet) {
                    Integer direction = vehicleDirections.get(vehicleId);
                    if (direction != null) {
                        if (direction == 1) {
                            upCount++;
                        } else if (direction == 2) {
                            downCount++;
                        }
                    }
                }
            }
        }

        private int getTotalUniqueVehicles() {
            Set<Long> allVehicles = new HashSet<>();
            for (Set<Long> vehicleSet : twoHourWindows.values()) {
                allVehicles.addAll(vehicleSet);
            }
            return allVehicles.size();
        }
    }

    // ==================== 每日详细交通量聚合器（按两小时去重）====================
    private static class DailyDetailedTrafficAggregator implements AggregateFunction<
            Tuple7<String, String, Integer, Long, Integer, Integer, Long>,
            DailyDetailedTrafficAccumulator,
            Tuple6<String, String, Integer, Integer, Integer, Integer>> {

        @Override
        public DailyDetailedTrafficAccumulator createAccumulator() {
            return new DailyDetailedTrafficAccumulator();
        }

        @Override
        public DailyDetailedTrafficAccumulator add(Tuple7<String, String, Integer, Long, Integer, Integer, Long> value, DailyDetailedTrafficAccumulator acc) {
            if (acc.dayKey == null) {
                acc.dayKey = value.f0;
                acc.stakeMark = value.f1;
                acc.direction = value.f2;
                logger.fine("开始处理每日详细交通量: " + value.f0 + "_" + value.f1 + "_" + value.f2);
            }
            acc.addVehicle(value.f3, value.f4, value.f5, value.f6);
            return acc;
        }

        @Override
        public Tuple6<String, String, Integer, Integer, Integer, Integer> getResult(DailyDetailedTrafficAccumulator acc) {
            int totalVehicles = acc.busCount + acc.trackCount + acc.otherCount;
            logger.info(String.format("每日详细交通量 %s_%s_%d 计算完成 - 总车辆数: %,d (客车: %,d, 货车: %,d, 其他: %,d)",
                    acc.dayKey, acc.stakeMark, acc.direction, totalVehicles,
                    acc.busCount, acc.trackCount, acc.otherCount));
            return Tuple6.of(acc.dayKey, acc.stakeMark, acc.direction,
                    acc.busCount, acc.trackCount, acc.otherCount);
        }

        @Override
        public DailyDetailedTrafficAccumulator merge(DailyDetailedTrafficAccumulator a, DailyDetailedTrafficAccumulator b) {
            logger.fine("合并每日详细交通量累加器");
            for (Map.Entry<String, Set<Long>> entry : b.twoHourWindows.entrySet()) {
                String twoHourKey = entry.getKey();
                Set<Long> vehicleSet = a.twoHourWindows.computeIfAbsent(twoHourKey, k -> new HashSet<>());
                vehicleSet.addAll(entry.getValue());
            }
            a.recalculateCounts();
            return a;
        }
    }

    private static class DailyDetailedTrafficAccumulator {
        public String dayKey;
        public String stakeMark; // 桩号标记，如K1016
        public int direction;
        // 两小时窗口 -> 车辆ID集合
        public Map<String, Set<Long>> twoHourWindows = new HashMap<>();
        public int busCount = 0;
        public int trackCount = 0;
        public int otherCount = 0;
        // 临时存储车辆类型信息
        private Map<Long, Integer> vehicleBusMap = new HashMap<>();
        private Map<Long, Integer> vehicleTrackMap = new HashMap<>();
        private int addCount = 0;

        public void addVehicle(long vehicleId, int isBus, int isTrack, long timestamp) {
            addCount++;

            // 存储车辆类型信息
            vehicleBusMap.put(vehicleId, isBus);
            vehicleTrackMap.put(vehicleId, isTrack);

            // 将时间戳转换为两小时窗口的起始时间字符串
            String twoHourKey = getTwoHourWindowKey(timestamp);
            Set<Long> vehicleSet = twoHourWindows.computeIfAbsent(twoHourKey, k -> new HashSet<>());

            if (!vehicleSet.contains(vehicleId)) {
                vehicleSet.add(vehicleId);
                if (isBus == 1) {
                    busCount++;
                } else if (isTrack == 1) {
                    trackCount++;
                } else {
                    otherCount++;
                }

                if (addCount % 5000 == 0) {
                    logger.fine(String.format("每日详细交通量 %s_%s_%d 处理进度 - 已处理: %,d, 唯一车辆: %,d",
                            dayKey, stakeMark, direction, addCount, getTotalUniqueVehicles()));
                }
            }
        }

        private String getTwoHourWindowKey(long timestamp) {
            // 将时间戳转换为两小时窗口的起始时间
            Date date = new Date(timestamp);
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(date);
            int hour = calendar.get(Calendar.HOUR_OF_DAY);
            hour = (hour / 2) * 2; // 取整到两小时
            calendar.set(Calendar.HOUR_OF_DAY, hour);
            calendar.set(Calendar.MINUTE, 0);
            calendar.set(Calendar.SECOND, 0);
            calendar.set(Calendar.MILLISECOND, 0);
            return new SimpleDateFormat("yyyyMMddHH").format(calendar.getTime());
        }

        public void recalculateCounts() {
            busCount = 0;
            trackCount = 0;
            otherCount = 0;

            for (Set<Long> vehicleSet : twoHourWindows.values()) {
                for (Long vehicleId : vehicleSet) {
                    Integer isBus = vehicleBusMap.get(vehicleId);
                    Integer isTrack = vehicleTrackMap.get(vehicleId);

                    if (isBus != null && isBus == 1) {
                        busCount++;
                    } else if (isTrack != null && isTrack == 1) {
                        trackCount++;
                    } else {
                        otherCount++;
                    }
                }
            }
        }

        private int getTotalUniqueVehicles() {
            Set<Long> allVehicles = new HashSet<>();
            for (Set<Long> vehicleSet : twoHourWindows.values()) {
                allVehicles.addAll(vehicleSet);
            }
            return allVehicles.size();
        }
    }

    // 简单的原子Double类
    private static class AtomicDouble {
        private double value = 0.0;

        public AtomicDouble(double v) {
            value=v;
        }

        public void addAndGet(double delta) {
            synchronized (this) {
                value += delta;
            }
        }

        public double get() {
            synchronized (this) {
                return value;
            }
        }
    }

    // ==================== HBase Sink 实现 ====================
    // 总交通量Sink
    private static class TotalHBaseTrafficSink extends RichSinkFunction<Tuple3<String, Integer, Integer>> {
        private static final Logger sinkLogger = Logger.getLogger(TotalHBaseTrafficSink.class.getName());
        private Connection connection;
        private Table table;
        private long writeCount = 0;
        private long startTime;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            startTime = System.currentTimeMillis();
            logger.info("初始化总交通量HBase Sink连接...");
            logger.info("表名: " + TABLE_NAME_TOTAL);
            logger.info("列族: " + COLUMN_FAMILY);

            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142,10.48.53.80");
            conf.set("hbase.zookeeper.property.clientPort", "2181");

            try {
                logger.info("正在创建HBase连接...");
                connection = ConnectionFactory.createConnection(conf);
                logger.info("HBase连接创建成功");

                createTableIfNotExists(TABLE_NAME_TOTAL, connection);
                table = connection.getTable(TableName.valueOf(TABLE_NAME_TOTAL));
                logger.info("HBase表 " + TABLE_NAME_TOTAL + " 打开成功");
                logger.info("总交通量HBase Sink初始化完成");
            } catch (Exception e) {
                logger.log(Level.SEVERE, "初始化总交通量HBase连接失败", e);
                throw e;
            }
        }

        @Override
        public void invoke(Tuple3<String, Integer, Integer> value, Context context) throws Exception {
            String rowKey = value.f0; // yyyyMMddHH格式
            int upCount = value.f1;
            int downCount = value.f2;

            try {
                Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("upcount"), Bytes.toBytes(String.valueOf(upCount)));
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("downcount"), Bytes.toBytes(String.valueOf(downCount)));

                table.put(put);
                writeCount++;

                // 每10次写入输出一次详细日志
                if (writeCount % 10 == 0) {
                    long elapsed = System.currentTimeMillis() - startTime;
                    double rate = writeCount / (elapsed / 1000.0);
                    logger.info(String.format("总交通量HBase写入统计 - 总写入次数: %,d, 当前写入: %s, 速率: %.2f 条/秒",
                            writeCount, rowKey, rate));
                }
            } catch (Exception e) {
                logger.log(Level.SEVERE, "写入总交通量HBase失败 - RowKey: " + rowKey, e);
                throw e;
            }
        }

        @Override
        public void close() throws Exception {
            logger.info("关闭总交通量HBase Sink资源...");
            logger.info("总计写入次数: " + writeCount);

            long elapsed = System.currentTimeMillis() - startTime;
            logger.info("总运行时间: " + (elapsed / 1000.0) + " 秒");

            try {
                if (table != null) {
                    table.close();
                    logger.info("HBase表连接已关闭");
                }
                if (connection != null && !connection.isClosed()) {
                    connection.close();
                    logger.info("HBase连接已关闭");
                }
            } catch (Exception e) {
                logger.log(Level.SEVERE, "关闭HBase资源时发生错误", e);
                throw e;
            }
            logger.info("总交通量HBase Sink资源已释放");
        }
    }

    // 详细交通量Sink - 修改为按桩号存储
    private static class DetailedHBaseTrafficSink extends RichSinkFunction<Tuple6<String, String, Integer, Integer, Integer, Integer>> {
        private static final Logger sinkLogger = Logger.getLogger(DetailedHBaseTrafficSink.class.getName());
        private Connection connection;
        private Table table;
        private long writeCount = 0;
        private long startTime;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            startTime = System.currentTimeMillis();
            logger.info("初始化详细交通量HBase Sink连接...");
            logger.info("表名: " + TABLE_NAME_DETAIL);

            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142,10.48.53.80");
            conf.set("hbase.zookeeper.property.clientPort", "2181");

            try {
                connection = ConnectionFactory.createConnection(conf);
                logger.info("HBase连接创建成功");

                createTableIfNotExists(TABLE_NAME_DETAIL, connection);
                table = connection.getTable(TableName.valueOf(TABLE_NAME_DETAIL));
                logger.info("HBase表 " + TABLE_NAME_DETAIL + " 打开成功");
                logger.info("详细交通量HBase Sink初始化完成");
            } catch (Exception e) {
                logger.log(Level.SEVERE, "初始化详细交通量HBase连接失败", e);
                throw e;
            }
        }

        @Override
        public void invoke(Tuple6<String, String, Integer, Integer, Integer, Integer> value, Context context) throws Exception {
            String rowKey = value.f1 + "_" + value.f0 + "_" + value.f2; // 桩号_小时_方向
            int busCount = value.f3;
            int trackCount = value.f4;
            int otherCount = value.f5;

            try {
                Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("bus_count"), Bytes.toBytes(String.valueOf(busCount)));
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("track_count"), Bytes.toBytes(String.valueOf(trackCount)));
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("other_count"), Bytes.toBytes(String.valueOf(otherCount)));

                table.put(put);
                writeCount++;

                if (writeCount % 20 == 0) {
                    long elapsed = System.currentTimeMillis() - startTime;
                    double rate = writeCount / (elapsed / 1000.0);
                    logger.info(String.format("详细交通量HBase写入统计 - 总写入次数: %,d, 速率: %.2f 条/秒",
                            writeCount, rate));
                }
            } catch (Exception e) {
                logger.log(Level.SEVERE, "写入详细交通量HBase失败 - RowKey: " + rowKey, e);
                throw e;
            }
        }

        @Override
        public void close() throws Exception {
            logger.info("关闭详细交通量HBase Sink资源...");
            logger.info("总计写入次数: " + writeCount);

            try {
                if (table != null) {
                    table.close();
                    logger.info("HBase表连接已关闭");
                }
                if (connection != null) {
                    connection.close();
                    logger.info("HBase连接已关闭");
                }
            } catch (Exception e) {
                logger.log(Level.SEVERE, "关闭HBase资源时发生错误", e);
                throw e;
            }
            logger.info("详细交通量HBase Sink资源已释放");
        }
    }

    // 匝道交通量Sink
    private static class RampHBaseTrafficSink extends RichSinkFunction<Tuple7<String, String, Integer, Integer, Integer, Double, Integer>> {
        private static final Logger sinkLogger = Logger.getLogger(RampHBaseTrafficSink.class.getName());
        private Connection connection;
        private Table table;
        private long writeCount = 0;
        private long startTime;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            startTime = System.currentTimeMillis();
            logger.info("初始化匝道交通量HBase Sink连接...");
            logger.info("表名: " + TABLE_NAME_RAMP);

            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142,10.48.53.80");
            conf.set("hbase.zookeeper.property.clientPort", "2181");

            try {
                connection = ConnectionFactory.createConnection(conf);
                logger.info("HBase连接创建成功");

                createTableIfNotExists(TABLE_NAME_RAMP, connection);
                table = connection.getTable(TableName.valueOf(TABLE_NAME_RAMP));
                logger.info("HBase表 " + TABLE_NAME_RAMP + " 打开成功");
                logger.info("匝道交通量HBase Sink初始化完成");
            } catch (Exception e) {
                logger.log(Level.SEVERE, "初始化匝道交通量HBase连接失败", e);
                throw e;
            }
        }

        @Override
        public void invoke(Tuple7<String, String, Integer, Integer, Integer, Double, Integer> value, Context context) throws Exception {
            String rowKey = value.f0 + "_" + value.f1; // 小时_匝道编号
            int totalCount = value.f2;
            int busCount = value.f3;
            int trackCount = value.f4;
            double avgSpeed = value.f5;
            int allCount = value.f6;

            try {
                Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("total_count"), Bytes.toBytes(String.valueOf(totalCount)));
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("bus_count"), Bytes.toBytes(String.valueOf(busCount)));
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("track_count"), Bytes.toBytes(String.valueOf(trackCount)));
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("avg_speed"), Bytes.toBytes(String.valueOf(avgSpeed)));
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("all_count"), Bytes.toBytes(String.valueOf(allCount)));

                table.put(put);
                writeCount++;

                if (writeCount % 10 == 0) {
                    long elapsed = System.currentTimeMillis() - startTime;
                    double rate = writeCount / (elapsed / 1000.0);
                    logger.info(String.format("匝道交通量HBase写入统计 - 总写入次数: %,d, 速率: %.2f 条/秒",
                            writeCount, rate));
                }
            } catch (Exception e) {
                logger.log(Level.SEVERE, "写入匝道交通量HBase失败 - RowKey: " + rowKey, e);
                throw e;
            }
        }

        @Override
        public void close() throws Exception {
            logger.info("关闭匝道交通量HBase Sink资源...");
            logger.info("总计写入次数: " + writeCount);

            try {
                if (table != null) {
                    table.close();
                    logger.info("HBase表连接已关闭");
                }
                if (connection != null) {
                    connection.close();
                    logger.info("HBase连接已关闭");
                }
            } catch (Exception e) {
                logger.log(Level.SEVERE, "关闭HBase资源时发生错误", e);
                throw e;
            }
            logger.info("匝道交通量HBase Sink资源已释放");
        }
    }

    // 每日总交通量Sink
    private static class DailyTotalHBaseTrafficSink extends RichSinkFunction<Tuple3<String, Integer, Integer>> {
        private static final Logger sinkLogger = Logger.getLogger(DailyTotalHBaseTrafficSink.class.getName());
        private Connection connection;
        private Table table;
        private long writeCount = 0;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            logger.info("初始化每日总交通量HBase Sink连接...");
            logger.info("表名: " + TABLE_NAME_DAILY_TOTAL);

            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142,10.48.53.80");
            conf.set("hbase.zookeeper.property.clientPort", "2181");

            try {
                connection = ConnectionFactory.createConnection(conf);
                logger.info("HBase连接创建成功");

                createTableIfNotExists(TABLE_NAME_DAILY_TOTAL, connection);
                table = connection.getTable(TableName.valueOf(TABLE_NAME_DAILY_TOTAL));
                logger.info("HBase表 " + TABLE_NAME_DAILY_TOTAL + " 打开成功");
                logger.info("每日总交通量HBase Sink初始化完成");
            } catch (Exception e) {
                logger.log(Level.SEVERE, "初始化每日总交通量HBase连接失败", e);
                throw e;
            }
        }

        @Override
        public void invoke(Tuple3<String, Integer, Integer> value, Context context) throws Exception {
            String rowKey = value.f0; // yyyyMMdd格式
            int upCount = value.f1;
            int downCount = value.f2;

            try {
                Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("upcount"), Bytes.toBytes(String.valueOf(upCount)));
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("downcount"), Bytes.toBytes(String.valueOf(downCount)));

                table.put(put);
                writeCount++;

                logger.info(String.format("写入每日总交通量: %s (上行: %,d, 下行: %,d)",
                        rowKey, upCount, downCount));
            } catch (Exception e) {
                logger.log(Level.SEVERE, "写入每日总交通量HBase失败 - RowKey: " + rowKey, e);
                throw e;
            }
        }

        @Override
        public void close() throws Exception {
            logger.info("关闭每日总交通量HBase Sink资源...");
            logger.info("总计写入次数: " + writeCount);

            try {
                if (table != null) {
                    table.close();
                    logger.info("HBase表连接已关闭");
                }
                if (connection != null) {
                    connection.close();
                    logger.info("HBase连接已关闭");
                }
            } catch (Exception e) {
                logger.log(Level.SEVERE, "关闭HBase资源时发生错误", e);
                throw e;
            }
            logger.info("每日总交通量HBase Sink资源已释放");
        }
    }

    // 每日详细交通量Sink
    private static class DailyDetailedHBaseTrafficSink extends RichSinkFunction<Tuple6<String, String, Integer, Integer, Integer, Integer>> {
        private static final Logger sinkLogger = Logger.getLogger(DailyDetailedHBaseTrafficSink.class.getName());
        private Connection connection;
        private Table table;
        private long writeCount = 0;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            logger.info("初始化每日详细交通量HBase Sink连接...");
            logger.info("表名: " + TABLE_NAME_DAILY_DETAIL);

            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142,10.48.53.80");
            conf.set("hbase.zookeeper.property.clientPort", "2181");

            try {
                connection = ConnectionFactory.createConnection(conf);
                logger.info("HBase连接创建成功");

                createTableIfNotExists(TABLE_NAME_DAILY_DETAIL, connection);
                table = connection.getTable(TableName.valueOf(TABLE_NAME_DAILY_DETAIL));
                logger.info("HBase表 " + TABLE_NAME_DAILY_DETAIL + " 打开成功");
                logger.info("每日详细交通量HBase Sink初始化完成");
            } catch (Exception e) {
                logger.log(Level.SEVERE, "初始化每日详细交通量HBase连接失败", e);
                throw e;
            }
        }

        @Override
        public void invoke(Tuple6<String, String, Integer, Integer, Integer, Integer> value, Context context) throws Exception {
            String rowKey = value.f0 + "_" + value.f1 + "_" + value.f2; // 日期_桩号_方向
            int busCount = value.f3;
            int trackCount = value.f4;
            int otherCount = value.f5;

            try {
                Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("bus_count"), Bytes.toBytes(String.valueOf(busCount)));
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("track_count"), Bytes.toBytes(String.valueOf(trackCount)));
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("other_count"), Bytes.toBytes(String.valueOf(otherCount)));

                table.put(put);
                writeCount++;

                if (writeCount % 10 == 0) {
                    logger.info(String.format("每日详细交通量HBase写入统计 - 总写入次数: %,d, 当前写入: %s",
                            writeCount, rowKey));
                }
            } catch (Exception e) {
                logger.log(Level.SEVERE, "写入每日详细交通量HBase失败 - RowKey: " + rowKey, e);
                throw e;
            }
        }

        @Override
        public void close() throws Exception {
            logger.info("关闭每日详细交通量HBase Sink资源...");
            logger.info("总计写入次数: " + writeCount);

            try {
                if (table != null) {
                    table.close();
                    logger.info("HBase表连接已关闭");
                }
                if (connection != null) {
                    connection.close();
                    logger.info("HBase连接已关闭");
                }
            } catch (Exception e) {
                logger.log(Level.SEVERE, "关闭HBase资源时发生错误", e);
                throw e;
            }
            logger.info("每日详细交通量HBase Sink资源已释放");
        }
    }

    // ==================== 通用工具方法 ====================
    /**
     * 检查并创建HBase表的工具方法
     * @param tableName 表名
     * @param connection HBase连接
     */
    private static void createTableIfNotExists(String tableName, Connection connection) {
        logger.info("检查HBase表是否存在: " + tableName);
        tableLock.lock();
        try (Admin admin = connection.getAdmin()) {
            TableName hbaseTableName = TableName.valueOf(tableName);

            Object lock = tableCreationLocks.computeIfAbsent(tableName, k -> new Object());

            synchronized (lock) {
                if (!admin.tableExists(hbaseTableName)) {
                    logger.info("表 " + tableName + " 不存在，正在创建...");
                    HTableDescriptor tableDescriptor = new HTableDescriptor(hbaseTableName);
                    tableDescriptor.addFamily(new HColumnDescriptor(COLUMN_FAMILY));
                    try {
                        admin.createTable(tableDescriptor);
                        logger.info("成功创建表: " + tableName);

                        // 等待表可用
                        int waitCount = 0;
                        while (!admin.isTableAvailable(hbaseTableName) && waitCount < 10) {
                            logger.info("等待表 " + tableName + " 可用... (" + (waitCount + 1) + "/10)");
                            Thread.sleep(1000);
                            waitCount++;
                        }

                        if (admin.isTableAvailable(hbaseTableName)) {
                            logger.info("表 " + tableName + " 现在可用");
                        } else {
                            logger.warning("表 " + tableName + " 创建后仍然不可用");
                        }
                    } catch (TableExistsException e) {
                        logger.warning("表已存在 (可能是并发创建): " + tableName);
                    }
                } else {
                    logger.info("表已存在: " + tableName);

                    // 检查表状态
                    if (admin.isTableEnabled(hbaseTableName)) {
                        logger.info("表 " + tableName + " 处于启用状态");
                    } else {
                        logger.warning("表 " + tableName + " 处于禁用状态");
                        try {
                            admin.enableTable(hbaseTableName);
                            logger.info("已启用表: " + tableName);
                        } catch (Exception e) {
                            logger.log(Level.WARNING, "启用表失败: " + tableName, e);
                        }
                    }
                }
            }
        } catch (IOException | InterruptedException e) {
            logger.log(Level.SEVERE, "检查/创建HBase表时发生错误: " + tableName, e);
        } finally {
            tableLock.unlock();
        }
    }
}