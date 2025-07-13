package whu.edu.moniData;

import com.google.gson.Gson;
import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import whu.edu.moniData.Utils.TrafficEventUtils;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class FiberGratingData6AllData {
    // 静态初始化器
    private static TrafficEventUtils.MileageConverter mileageConverter1;
    private static TrafficEventUtils.MileageConverter mileageConverter2;
    static {
        try {
            mileageConverter1 = new TrafficEventUtils.MileageConverter("sx_json.json");
            mileageConverter2 = new TrafficEventUtils.MileageConverter("xx_json.json");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // 输出重定向
    private static PrintStream originalOut;
    private static PrintStream originalErr;
    private static void redirectOutputToFile() {
        try {
            originalOut = System.out;
            originalErr = System.err;
            FileOutputStream fos = new FileOutputStream("/home/ljj/debugx.txt", true);
            PrintStream printStream = new PrintStream(fos);
            System.setOut(printStream);
            System.setErr(printStream);
            System.out.println("\n\n========== 新的运行会话开始 ==========");
        } catch (FileNotFoundException e) {
            System.err.println("无法创建输出文件: " + e.getMessage());
            System.setOut(originalOut);
            System.setErr(originalErr);
        }
    }

    // 车辆生成配置
    private static final List<String> PROVINCE_CODES = Arrays.asList(
            "京", "沪", "津", "渝", "冀", "晋", "蒙", "辽", "吉", "黑",
            "苏", "浙", "皖", "闽", "赣", "鲁", "豫", "鄂", "湘", "粤",
            "桂", "琼", "川", "贵", "云", "藏", "陕", "甘", "青", "宁"
    );
    private static final String CITY_LETTERS = "ABCDEFGH";
    private static final String UPPER_CASE_LETTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private static final long MIN_MILEAGE = 1016020;
    private static final long MAX_MILEAGE = 1173790;
    private static final long[] SEGMENT_ENDS = {
            1037954, 1048271, 1068083, 1083768, 1099029,
            1115410, 1125320, 1140176, 1156422, 1166689, 1173790
    };
    private static final String[] TOPICS = new String[11];
    static {
        for (int i = 0; i < 11; i++) {
            TOPICS[i] = "fiberData" + (i + 1);
        }
    }

    // Kafka生产者
    private static final List<KafkaProducerUtil> producers = new ArrayList<>();
    static {
        for (String topic : TOPICS) {
            producers.add(new KafkaProducerUtil(topic));
        }
    }
    private static final String TOLL_TOPIC = "tollData";
    private static final KafkaProducerUtil producerToll = new KafkaProducerUtil(TOLL_TOPIC);

    // 配置参数
    private static final DecimalFormat SPEED_FORMAT = new DecimalFormat("#.00");
    private static final double SPEED_CHANGE_PROBABILITY = 0.2;
    private static final double SPEED_CHANGE_RANGE = 0.1;
    private static final double DATA_POINT_MISSING_PROBABILITY = 0;
    private static final double ID_CHANGE_PROBABILITY = 0;
    private static final double WAYNO_CHANGE_PROBABILITY = 0;
    private static final int MIN_SAFE_DISTANCE = 10;
    private static final double NOISE_POINT_PROBABILITY = 0;
    private static final int PEAK_START_MORNING = 7;
    private static final int PEAK_END_MORNING = 9;
    private static final int PEAK_START_EVENING = 17;
    private static final int PEAK_END_EVENING = 19;
    private static int PEAK_INITIAL_VEHICLES = 3;
    private static int OFFPEAK_INITIAL_VEHICLES = 3;

    // 性能监控参数
    private static final int WARNING_THRESHOLD_MS = 150;
    private static final int CRITICAL_THRESHOLD_MS = 180;
    private static final int MAX_DYNAMIC_ADJUST = 50;

    // 智能概率调整参数 - 优化后
    private static final int IDEAL_VEHICLE_COUNT = 150;  // 提高到150辆
    private static final int MAX_VEHICLES = 200;         // 绝对上限
    private static final double MIN_PROB = 0.03;         // 最小概率
    private static final double MAX_PROB = 0.9;          // 最大概率
    private static final int ADJUST_INTERVAL = 50;
    private static double PEAK_INCOMING_PROB_BASE = 0.6 / 2;    // 提高基准概率
    private static double OFFPEAK_INCOMING_PROB_BASE = 0.2 / 2;
    private static double PEAK_INCOMING_PROB = PEAK_INCOMING_PROB_BASE;
    private static double OFFPEAK_INCOMING_PROB = OFFPEAK_INCOMING_PROB_BASE;

    // 新增：动态间隔控制
    private static int currentInterval = 200; // 初始间隔200ms

    // 卡口配置
    private static class TollGate {
        String id;
        long mileage;
        public TollGate(String id, long mileage) {
            this.id = id;
            this.mileage = mileage;
        }
    }
    private static final List<TollGate> UP_Toll_GATES = Arrays.asList(
            new TollGate("KKJK-02", 1030900), new TollGate("KKJK-04", 1033700),
            new TollGate("KKJK-06", 1043550), new TollGate("KKJK-08", 1058350),
            new TollGate("KKJK-10", 1062600), new TollGate("KKJK-12", 1063250),
            new TollGate("KKJK-14", 1075600), new TollGate("KKJK-15", 1086450),
            new TollGate("KKJK-18", 1092600), new TollGate("KKJK-20", 1110150),
            new TollGate("KKJK-22", 1112750), new TollGate("KKJK-25", 1115550),
            new TollGate("KKJK-26", 1116300), new TollGate("KKJK-28", 1122800),
            new TollGate("KKJK-30", 1129300), new TollGate("KKJK-33", 1140950),
            new TollGate("KKJK-35", 1146800), new TollGate("KKJK-38", 1149800),
            new TollGate("KKJK-40", 1154400), new TollGate("KKJK-42", 1162600),
            new TollGate("KKJK-43", 1163300), new TollGate("KKJK-45", 1165450),
            new TollGate("KKJK-47", 1168400), new TollGate("KKJK-49", 1173450)
    );
    private static final List<TollGate> DOWN_Toll_GATES = Arrays.asList(
            new TollGate("KKJK-01", 1030000), new TollGate("KKJK-03", 1032900),
            new TollGate("KKJK-05", 1043200), new TollGate("KKJK-07", 1057800),
            new TollGate("KKJK-09", 1061800), new TollGate("KKJK-11", 1062850),
            new TollGate("KKJK-13", 1074820), new TollGate("KKJK-16", 1086730),
            new TollGate("KKJK-17", 1092150), new TollGate("KKJK-19", 1109600),
            new TollGate("KKJK-21", 1111780), new TollGate("KKJK-23", 1114750),
            new TollGate("KKJK-24", 1115500), new TollGate("KKJK-27", 1121900),
            new TollGate("KKJK-29", 1128800), new TollGate("KKJK-31", 1139900),
            new TollGate("KKJK-32", 1140400), new TollGate("KKJK-34", 1145700),
            new TollGate("KKJK-36", 1148064), new TollGate("KKJK-37", 1148600),
            new TollGate("KKJK-39", 1153400), new TollGate("KKJK-41", 1161500),
            new TollGate("KKJK-44", 1164560), new TollGate("KKJK-46", 1168000),
            new TollGate("KKJK-48", 1172890)
    );

    // 车辆数据类 - 优化后
    @Getter
    @Setter
    static class TData {
        int id;
        String plateNo;
        int vehicleType;
        double speed;
        int laneNo;
        long mileage;
        int direction;
        @Getter String stakeId;
        @Getter private final Set<String> passedTollGates;
        double longitude;
        double latitude;

        // 优化缓存字段
        private transient int segmentIndex = -1;
        private transient boolean isOnBridge = false;
        private transient TollGate nearestTollGate = null;
        private transient long distanceToNextTollGate = Long.MAX_VALUE;

        public TData(int id, String carNumber, int vehicleType, double speed, int laneNo,
                     long mileage, int direction) {
            this.id = id;
            this.plateNo = carNumber;
            this.vehicleType = vehicleType;
            this.speed = formatSpeed(speed);
            this.laneNo = laneNo;
            this.mileage = mileage;
            this.direction = direction;
            this.stakeId = getStakeNumber();
            this.passedTollGates = new HashSet<>();
            double[] lnglat = generateInitialJingWei(mileage, direction);
            this.longitude = lnglat[0];
            this.latitude = lnglat[1];
            updateCachedProperties();
        }

        public TData(TData original, int newWayno, long newTpointno, double newSpeed, boolean idChanged) {
            this.id = idChanged ? Math.abs(UUID.randomUUID().hashCode()) : original.id;
            this.plateNo = original.plateNo;
            this.vehicleType = original.vehicleType;
            this.speed = formatSpeed(newSpeed);
            this.laneNo = newWayno;
            this.mileage = newTpointno;
            this.direction = original.direction;
            this.stakeId = getStakeNumber();
            this.passedTollGates = new HashSet<>(original.passedTollGates);
            double[] lnglat = generateInitialJingWei(newTpointno, original.direction);
            this.longitude = lnglat[0];
            this.latitude = lnglat[1];
            updateCachedProperties();
        }

        // 优化后的缓存计算
        private void updateCachedProperties() {
            // 计算分段索引
            segmentIndex = 0;
            while (segmentIndex < SEGMENT_ENDS.length && mileage > SEGMENT_ENDS[segmentIndex]) {
                segmentIndex++;
            }

            // 计算是否在桥上
            isOnBridge = (mileage >= 1050000 && mileage <= 1055000);

            // 更新最近的卡口信息
            updateNearestTollGate();
        }

        // 更新最近的卡口信息
        private void updateNearestTollGate() {
            List<TollGate> relevantGates = (direction == 1) ? UP_Toll_GATES : DOWN_Toll_GATES;
            TollGate nearest = null;
            long minDistance = Long.MAX_VALUE;

            for (TollGate gate : relevantGates) {
                if (passedTollGates.contains(gate.id)) continue;

                long distance = Math.abs(mileage - gate.mileage);
                if (distance < minDistance) {
                    minDistance = distance;
                    nearest = gate;
                }
            }

            this.nearestTollGate = nearest;
            this.distanceToNextTollGate = minDistance;
        }

        public String getStakeNumber() {
            long km = (mileage / 1000);
            long meter = (mileage % 1000);
            return String.format("K%d+%03d", km, meter);
        }

        private static double formatSpeed(double speed) {
            return Double.parseDouble(SPEED_FORMAT.format(speed));
        }
    }

    // 光纤数据类
    public static class FiberGratingJsonData {
        int SN;
        String timeStamp;
        int pathNum;
        List<TData> pathList;

        public FiberGratingJsonData(int sn, long time, List<TData> tdata) {
            this.SN = sn;
            this.timeStamp = formatTimestamp(time);
            this.pathList = tdata;
            this.pathNum = tdata.size();
        }

        public static String formatTimestamp(long timestamp) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
            return sdf.format(new Date(timestamp));
        }
    }

    // ======================= 优化后的核心方法 =======================

    // 优化后的并行处理方法
    private static void parallelProcessSegments(List<List<TData>> segmentVehicles,
                                                FiberGratingJsonData originalData) {
        int processors = Runtime.getRuntime().availableProcessors();
        int poolSize = Math.min(processors * 2, Math.min(32, segmentVehicles.size()));

        ExecutorService executor = Executors.newFixedThreadPool(poolSize);

        for (int i = 0; i < segmentVehicles.size(); i++) {
            final int segmentIndex = i;
            List<TData> segmentData = segmentVehicles.get(segmentIndex);

            if (!segmentData.isEmpty()) {
                executor.submit(() -> {
                    try {
                        long timeObs = parseTimestamp(originalData.timeStamp);
                        FiberGratingJsonData segmentJsonData = new FiberGratingJsonData(
                                originalData.SN, timeObs, segmentData
                        );
                        producers.get(segmentIndex).sendData(segmentJsonData);
                    } catch (Exception e) {
                        System.err.println("分段处理错误: " + e.getMessage());
                    }
                });
            }
        }

        executor.shutdown();
        try {
            executor.awaitTermination(100, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // 优化后的车辆位置更新方法
    private static List<TData> updateVehiclePositions(List<TData> vehicles, double elapsedTime, Random random) {
        return vehicles.parallelStream()
                .map(vehicle -> {
                    // 为每个车辆创建独立的随机数生成器
                    Random vehicleRandom = new Random(random.nextLong());

                    // 速度变化逻辑
                    double newSpeed = vehicle.speed;
                    if (vehicleRandom.nextDouble() < SPEED_CHANGE_PROBABILITY) {
                        double change = vehicleRandom.nextDouble() * 2 * SPEED_CHANGE_RANGE - SPEED_CHANGE_RANGE;
                        newSpeed = Math.max(80, Math.min(120, vehicle.speed * (1 + change)));
                    }

                    // 位置更新
                    int directionFactor = (vehicle.direction == 1) ? 1 : -1;
                    long positionChange = (long) (newSpeed / 3.6 * elapsedTime * directionFactor);
                    long newTpointno = vehicle.mileage + positionChange;

                    // 边界检查 - 移除超出边界的车辆
                    if ((vehicle.direction == 1 && newTpointno > MAX_MILEAGE) ||
                            (vehicle.direction == 2 && newTpointno < MIN_MILEAGE)) {
                        return null;
                    }

                    // 车道变更逻辑
                    boolean idChanged = vehicleRandom.nextDouble() < ID_CHANGE_PROBABILITY;
                    int newWayno = vehicle.laneNo;

                    if (elapsedTime > 5 && vehicleRandom.nextDouble() < WAYNO_CHANGE_PROBABILITY) {
                        int candidateWayno = newWayno + (vehicleRandom.nextBoolean() ? 1 : -1);
                        if (candidateWayno >= 1 && candidateWayno <= 4) {
                            newWayno = candidateWayno;
                        }
                    }

                    // 创建更新后的车辆对象
                    return new TData(vehicle, newWayno, newTpointno, newSpeed, idChanged);
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    // 优化后的智能概率调整方法
    private static void adjustIncomingProbability(int currentVehicleCount, long durationMs) {
        // 1. 使用对数函数平滑密度因子
        double densityFactor = Math.log1p(currentVehicleCount) / Math.log1p(IDEAL_VEHICLE_COUNT);

        // 2. 动态调整概率
        double newPeakProb = PEAK_INCOMING_PROB_BASE / densityFactor;
        double newOffPeakProb = OFFPEAK_INCOMING_PROB_BASE / densityFactor;

        // 3. 应用学习因子
        double learningFactor = 1.0;
        if (durationMs > CRITICAL_THRESHOLD_MS) {
            learningFactor = 0.8; // 超时惩罚
        } else if (durationMs < WARNING_THRESHOLD_MS) {
            learningFactor = 1.1; // 性能奖励
        }

        // 4. 概率范围保护
        PEAK_INCOMING_PROB = Math.max(MIN_PROB, Math.min(MAX_PROB, newPeakProb * learningFactor));
        OFFPEAK_INCOMING_PROB = Math.max(MIN_PROB, Math.min(MAX_PROB, newOffPeakProb * learningFactor));

        // 5. 输出详细日志
        System.out.printf("智能调整: 车辆=%d, 密度因子=%.2f, 学习因子=%.1f, 新概率[高峰=%.4f, 平峰=%.4f], 处理时长=%dms%n",
                currentVehicleCount, densityFactor, learningFactor,
                PEAK_INCOMING_PROB, OFFPEAK_INCOMING_PROB,
                durationMs);

        // 6. 紧急降级机制
        if (durationMs > CRITICAL_THRESHOLD_MS * 1.5) {
            double emergencyFactor = 0.7;
            PEAK_INCOMING_PROB *= emergencyFactor;
            OFFPEAK_INCOMING_PROB *= emergencyFactor;
            System.out.printf("🚨 紧急降级: 处理时长 %dms > %dms, 概率额外降低30%% → [高峰=%.4f, 平峰=%.4f]%n",
                    durationMs, (int)(CRITICAL_THRESHOLD_MS * 1.5),
                    PEAK_INCOMING_PROB, OFFPEAK_INCOMING_PROB);
        }
    }

    private static void parseCommandLineArguments(String[] args) {
        if (args == null || args.length == 0) return;

        try {
            // 设置基准概率
            PEAK_INCOMING_PROB_BASE = Double.parseDouble(args[0]);
            if (args.length > 1) OFFPEAK_INCOMING_PROB_BASE = Double.parseDouble(args[1]);

            // 初始化当前概率为基准值
            PEAK_INCOMING_PROB = PEAK_INCOMING_PROB_BASE;
            OFFPEAK_INCOMING_PROB = OFFPEAK_INCOMING_PROB_BASE;

            if (args.length > 2) PEAK_INITIAL_VEHICLES = Integer.parseInt(args[2]);
            if (args.length > 3) OFFPEAK_INITIAL_VEHICLES = Integer.parseInt(args[3]);

            System.out.printf("参数设置: 基准概率[高峰=%.4f, 平峰=%.4f], 初始车辆[高峰=%d, 平峰=%d]%n",
                    PEAK_INCOMING_PROB_BASE, OFFPEAK_INCOMING_PROB_BASE,
                    PEAK_INITIAL_VEHICLES, OFFPEAK_INITIAL_VEHICLES);
        } catch (NumberFormatException e) {
            resetToDefaultValues();
        }
    }

    private static void resetToDefaultValues() {
        // 重置基准概率
        PEAK_INCOMING_PROB_BASE = 0.5 / 2;
        OFFPEAK_INCOMING_PROB_BASE = 0.15 / 2;

        // 重置当前概率
        PEAK_INCOMING_PROB = PEAK_INCOMING_PROB_BASE;
        OFFPEAK_INCOMING_PROB = OFFPEAK_INCOMING_PROB_BASE;

        PEAK_INITIAL_VEHICLES = 3;
        OFFPEAK_INITIAL_VEHICLES = 3;

        System.out.println("使用默认配置值");
    }

    // 优化后的主方法
    public static void main(String[] args) {
        redirectOutputToFile();
        parseCommandLineArguments(args);
        System.out.println("优化版轨迹模拟程序启动");
        System.out.printf("配置: 理想车辆数=%d, 最高车辆数=%d, 高峰概率=%.4f, 平峰概率=%.4f%n",
                IDEAL_VEHICLE_COUNT, MAX_VEHICLES,
                PEAK_INCOMING_PROB, OFFPEAK_INCOMING_PROB);

        final long baseTime = System.currentTimeMillis();
        int sn = 1;
        List<TData> activeVehicles = generateInitialVehicleData(new Random(), baseTime);
        int dynamicAdjustCount = 0;

        while (!Thread.currentThread().isInterrupted() && activeVehicles.size() <= MAX_VEHICLES) {
            final long loopStartNano = System.nanoTime();
            final long targetTime = getTargetTime(sn, baseTime, currentInterval);

            // 1. 更新车辆位置
            activeVehicles = updateVehiclePositions(activeVehicles, currentInterval / 1000.0, new Random());

            // 2. 移除超出范围的车辆
            activeVehicles = activeVehicles.stream()
                    .filter(vehicle ->
                            (vehicle.direction == 1 && vehicle.mileage < MAX_MILEAGE) ||
                                    (vehicle.direction == 2 && vehicle.mileage > MIN_MILEAGE))
                    .collect(Collectors.toList());

            // 3. 生成新车辆
            activeVehicles = generateIncomingVehicles(activeVehicles, new Random(), targetTime);

            // 4. 创建原始数据
            FiberGratingJsonData originalData = new FiberGratingJsonData(sn, targetTime, activeVehicles);

            // 5. 分段处理
            List<List<TData>> segmentVehicles = new ArrayList<>();
            for (int i = 0; i < 11; i++) segmentVehicles.add(new ArrayList<>());

            // 6. 卡口检测
            List<TollData> tollDataList = new ArrayList<>();

            // 7. 处理车辆数据（使用缓存优化）
            for (TData vehicle : activeVehicles) {
                if (vehicle.isOnBridge) continue;
                if (vehicle.segmentIndex >= 0 && vehicle.segmentIndex < 11) {
                    segmentVehicles.get(vehicle.segmentIndex).add(vehicle);
                }
                detectTollGates(vehicle, originalData.timeStamp, tollDataList);
            }

            // 8. 发送卡口数据
            if (!tollDataList.isEmpty()) {
                Gson gson = new Gson();
                String tollJson = gson.toJson(tollDataList);
                producerToll.sendData("tollData", String.valueOf(sn), tollJson);
            }

            // 9. 并行发送分段数据
            parallelProcessSegments(segmentVehicles, originalData);

            // 计算处理时长
            long durationMs = (System.nanoTime() - loopStartNano) / 1_000_000;

            // 10. 性能监控
            monitorPerformance(durationMs, sn, activeVehicles.size());

            // 11. 智能概率调整
            if (sn % ADJUST_INTERVAL == 0) {
                adjustIncomingProbability(activeVehicles.size(), durationMs);
            }

            // 12. 动态调整间隔
            currentInterval = applyDynamicIntervalAdjustment(durationMs, dynamicAdjustCount);

            // 13. 计算并等待
            long currentTime = System.currentTimeMillis();
            long sleepTime = (targetTime + currentInterval) - currentTime;

            if (sleepTime > 0) {
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            } else {
                System.out.printf("⏱️ SN%d 延迟: %dms%n", sn, -sleepTime);
                dynamicAdjustCount++;
            }

            sn++;
        }

        // 关闭生产者
        closeProducers();
        System.out.println("程序正常退出");
    }

    // ======================= 辅助方法 =======================

    // 优化后的性能监控方法
    private static void monitorPerformance(long durationMs, int sn, int vehicleCount) {
        System.out.printf("📊 SN%d: 车辆=%d, 处理时长=%dms%n", sn, vehicleCount, durationMs);

        if (durationMs > WARNING_THRESHOLD_MS) {
            System.out.printf("⚠️ 警告 SN%d: 处理耗时 %dms (超过阈值 %dms)%n",
                    sn, durationMs, WARNING_THRESHOLD_MS);
        }

        if (durationMs > CRITICAL_THRESHOLD_MS) {
            System.out.printf("🚨 严重 SN%d: 处理耗时 %dms (超过阈值 %dms)%n",
                    sn, durationMs, CRITICAL_THRESHOLD_MS);
        }
    }

    // 动态间隔调整
    private static int applyDynamicIntervalAdjustment(long durationMs, int adjustCount) {
        if (adjustCount > MAX_DYNAMIC_ADJUST) return 200;

        if (durationMs > CRITICAL_THRESHOLD_MS) {
            int newInterval = 200 + (int)(durationMs - CRITICAL_THRESHOLD_MS) / 2;
            newInterval = Math.max(100, Math.min(300, newInterval)); // 100-300ms范围
            System.out.printf("🛠️ 动态间隔调整: 处理超时%dms → 新间隔%dms%n",
                    durationMs - 200, newInterval);
            return newInterval;
        }
        return currentInterval; // 返回当前间隔值
    }

    // 优化后的卡口检测方法
    private static void detectTollGates(TData vehicle, String timestamp, List<TollData> tollDataList) {
        // 使用缓存字段进行高效检测
        if (vehicle.distanceToNextTollGate <= 20) {
            if (vehicle.nearestTollGate != null && !vehicle.getPassedTollGates().contains(vehicle.nearestTollGate.id)) {
                tollDataList.add(new TollData(
                        vehicle.plateNo, vehicle.vehicleType,
                        formatToSecondPrecision(timestamp),
                        vehicle.nearestTollGate.id, vehicle.laneNo
                ));
                vehicle.getPassedTollGates().add(vehicle.nearestTollGate.id);
                vehicle.updateNearestTollGate(); // 更新下一个卡口
            }
        }
    }

    private static String formatToSecondPrecision(String timestampWithMs) {
        return timestampWithMs.split(":")[0] + ":" + timestampWithMs.split(":")[1] + ":" + timestampWithMs.split(":")[2];
    }

    private static boolean isPeakTime(long timestamp) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(timestamp);
        int hour = calendar.get(Calendar.HOUR_OF_DAY);
        return (hour >= PEAK_START_MORNING && hour < PEAK_END_MORNING) ||
                (hour >= PEAK_START_EVENING && hour < PEAK_END_EVENING);
    }

    // 卡口数据类 - 不变
    static class TollData {
        String plateNumber;
        int vehicleType;
        String uploadTime;
        String deviceId;
        int headLaneCode;
        public TollData(String plateNumber, int vehicleType, String uploadTime, String deviceId, int headLaneCode) {
            this.plateNumber = plateNumber;
            this.vehicleType = vehicleType;
            this.uploadTime = uploadTime;
            this.deviceId = deviceId;
            this.headLaneCode = headLaneCode;
        }
    }

    // ======================= 生产者工具类 =======================
    public static class KafkaProducerUtil implements AutoCloseable {
        private final String TOPIC;
        private final Producer<String, String> producer;
        private final Gson gson = new Gson();

        public KafkaProducerUtil(String topic) {
            this.TOPIC = topic;
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "100.65.38.40:9092");
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.ACKS_CONFIG, "1");
            props.put(ProducerConfig.RETRIES_CONFIG, 3);
            props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
            props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
            props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);

            this.producer = new KafkaProducer<>(props);
        }

        public void sendData(FiberGratingJsonData data) {
            String key = String.valueOf(data.SN);
            String value = gson.toJson(data);
            producer.send(new ProducerRecord<>(TOPIC, key, value));
        }

        public void sendData(String topic, String key, String jsonValue) {
            producer.send(new ProducerRecord<>(topic, key, jsonValue));
        }

        @Override
        public void close() {
            producer.flush();
            producer.close();
        }
    }

    // ======================= 私有辅助方法 =======================
    private static long getTargetTime(int sn, long baseTime, int interval) {
        return baseTime + sn * interval;
    }

    private static long parseTimestamp(String timestamp) {
        try {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS");
            LocalDateTime localDateTime = LocalDateTime.parse(timestamp, formatter);
            return localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        } catch (Exception e) {
            return System.currentTimeMillis();
        }
    }

    private static String generateCarNumber(Random random) {
        String provinceCode = PROVINCE_CODES.get(random.nextInt(PROVINCE_CODES.size()));
        char cityLetter = CITY_LETTERS.charAt(random.nextInt(CITY_LETTERS.length()));
        StringBuilder suffix = new StringBuilder();
        for (int i = 0; i < 5; i++) {
            if (random.nextDouble() < 0.8) {
                suffix.append(random.nextInt(10));
            } else {
                suffix.append(UPPER_CASE_LETTERS.charAt(random.nextInt(UPPER_CASE_LETTERS.length())));
            }
        }
        return provinceCode + cityLetter + suffix.toString();
    }

    private static List<TData> generateInitialVehicleData(Random random, long timestamp) {
        int initialCount = isPeakTime(timestamp) ? PEAK_INITIAL_VEHICLES : OFFPEAK_INITIAL_VEHICLES;
        List<TData> vehicles = new ArrayList<>();
        for (int i = 0; i < initialCount; i++) {
            int id = Math.abs(UUID.randomUUID().hashCode());
            String carNumber = generateCarNumber(random);
            double speed = generateInitialSpeed(random);
            int wayno = random.nextInt(4) + 1;
            long tpointno = generateInitialPosition(random);
            int direct = random.nextInt(2) + 1;
            vehicles.add(new TData(id, carNumber, random.nextInt(10), speed, wayno, tpointno, direct));
        }
        return ensureSafeInitialPositions(vehicles);
    }

    private static List<TData> ensureSafeInitialPositions(List<TData> vehicles) {
        Map<Integer, Map<Integer, List<TData>>> laneDirectionMap = new TreeMap<>();
        for (TData v : vehicles) {
            laneDirectionMap.computeIfAbsent(v.laneNo, k -> new HashMap<>())
                    .computeIfAbsent(v.direction, k -> new ArrayList<>())
                    .add(v);
        }

        List<TData> safeVehicles = new ArrayList<>();
        for (Map.Entry<Integer, Map<Integer, List<TData>>> laneEntry : laneDirectionMap.entrySet()) {
            for (Map.Entry<Integer, List<TData>> directionEntry : laneEntry.getValue().entrySet()) {
                int direction = directionEntry.getKey();
                List<TData> dirVehicles = directionEntry.getValue();
                dirVehicles.sort((v1, v2) ->
                        direction == 1 ? Long.compare(v1.mileage, v2.mileage) : Long.compare(v2.mileage, v1.mileage)
                );

                long prevPosition = (direction == 1) ? MIN_MILEAGE - MIN_SAFE_DISTANCE : MAX_MILEAGE + MIN_SAFE_DISTANCE;
                for (TData vehicle : dirVehicles) {
                    long newPosition;
                    if (direction == 1) {
                        newPosition = Math.max(vehicle.mileage, prevPosition + MIN_SAFE_DISTANCE);
                        newPosition = Math.min(newPosition, MAX_MILEAGE);
                    } else {
                        newPosition = Math.min(vehicle.mileage, prevPosition - MIN_SAFE_DISTANCE);
                        newPosition = Math.max(newPosition, MIN_MILEAGE);
                    }
                    safeVehicles.add(new TData(vehicle, vehicle.laneNo, newPosition, vehicle.speed, false));
                    prevPosition = newPosition;
                }
            }
        }
        return safeVehicles;
    }

    // ======================= 车辆生命周期管理 =======================
    private static List<TData> generateIncomingVehicles(List<TData> currentVehicles, Random random, long timestamp) {
        List<TData> newVehicles = new ArrayList<>(currentVehicles);
        double incomingProb = isPeakTime(timestamp) ? PEAK_INCOMING_PROB : OFFPEAK_INCOMING_PROB;

        // 动态调整概率：基于当前车辆数
        double dynamicProb = incomingProb * (1 + (IDEAL_VEHICLE_COUNT - currentVehicles.size()) / 100.0);
        dynamicProb = Math.max(MIN_PROB, Math.min(MAX_PROB, dynamicProb));

        if (random.nextDouble() < dynamicProb) {
            int lane = random.nextInt(4) + 1;
            if (isLaneSafeForEntry(currentVehicles, lane, 1, MIN_MILEAGE)) {
                newVehicles.add(createNewVehicle(lane, 1, random));
            }
        }

        if (random.nextDouble() < dynamicProb) {
            int lane = random.nextInt(4) + 1;
            if (isLaneSafeForEntry(currentVehicles, lane, 2, MAX_MILEAGE)) {
                newVehicles.add(createNewVehicle(lane, 2, random));
            }
        }

        return newVehicles;
    }

    private static boolean isLaneSafeForEntry(List<TData> vehicles, int lane, int direction, long entryPosition) {
        return vehicles.stream().noneMatch(v ->
                v.laneNo == lane && v.direction == direction &&
                        Math.abs(v.mileage - entryPosition) < MIN_SAFE_DISTANCE
        );
    }

    private static TData createNewVehicle(int lane, int direction, Random random) {
        long entryPosition = (direction == 1) ? MIN_MILEAGE : MAX_MILEAGE;
        return new TData(
                Math.abs(UUID.randomUUID().hashCode()),
                generateCarNumber(random),
                random.nextInt(10),
                generateInitialSpeed(random),
                lane,
                entryPosition,
                direction
        );
    }

    private static double generateInitialSpeed(Random random) {
        return TData.formatSpeed(80 + random.nextDouble() * 40);
    }

    private static long generateInitialPosition(Random random) {
        return 1049000 + (long) (random.nextDouble() * 900);
    }

    private static double[] generateInitialJingWei(long tpointno, int direc) {
        TrafficEventUtils.MileageConverter converter = (direc == 1) ? mileageConverter1 : mileageConverter2;
        return converter.findCoordinate((int) tpointno).getLnglat();
    }

    private static void closeProducers() {
        System.out.println("关闭Kafka生产者...");
        for (KafkaProducerUtil producer : producers) {
            producer.close();
        }
        producerToll.close();
    }
}