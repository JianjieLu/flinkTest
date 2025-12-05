package whu.edu.moniDataXinghu;

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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class MoniNoCrowded10ToCar {
    // 错误数据配置参数
    private static final double SPEED_CHANGE_PROBABILITY = 0.2;
    private static final double SPEED_CHANGE_RANGE = 0.1;
    private static final double DATA_POINT_MISSING_PROBABILITY = 0.01; // 1%数据点缺失
    private static final double ID_CHANGE_PROBABILITY = 0; // 0.01% ID突变
    private static final double WAYNO_CHANGE_PROBABILITY = 0.1; // 10%变道
    private static final double NOISE_POINT_PROBABILITY = 0; // 0.1%噪声点

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
    private static void redirectOutputToFile() {
        try {
            FileOutputStream fos = new FileOutputStream("/home/ljj/debugx.txt", true);
            PrintStream printStream = new PrintStream(fos);

            System.setOut(printStream);
            System.setErr(printStream);

            System.out.println("\n\n========== 新的运行会话开始 ==========");
        } catch (FileNotFoundException e) {
            System.err.println("无法创建输出文件: " + e.getMessage());
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
    private static final String SPECIAL_TOPIC = "specialTrafficInfo";
    private static final KafkaProducerUtil producerSpecial = new KafkaProducerUtil(SPECIAL_TOPIC);

    // 配置参数
    private static final DecimalFormat SPEED_FORMAT = new DecimalFormat("#.00");
    private static final DecimalFormat WEIGHT_FORMAT = new DecimalFormat("#.0");
    private static final int MIN_SAFE_DISTANCE = 10;
    private static final int PEAK_START_MORNING = 7;
    private static final int PEAK_END_MORNING = 9;
    private static final int PEAK_START_EVENING = 17;
    private static final int PEAK_END_EVENING = 19;
    private static int PEAK_INITIAL_VEHICLES = 3;
    private static int OFFPEAK_INITIAL_VEHICLES = 3;
    private static final int WARNING_THRESHOLD_MS = 150;
    private static final int CRITICAL_THRESHOLD_MS = 180;
    private static final int MAX_DYNAMIC_ADJUST = 50;
    //    private static final int IDEAL_VEHICLE_COUNT = 50;
    private static final int IDEAL_VEHICLE_COUNT = 100;
    private static final double MIN_PROB = 0.05;
    private static final double MAX_PROB = 0.8;
    private static final int ADJUST_INTERVAL = 50;
    private static double PEAK_INCOMING_PROB_BASE = 0.5 / 2;
    private static double OFFPEAK_INCOMING_PROB_BASE = 0.15 / 2;
    private static double PEAK_INCOMING_PROB = PEAK_INCOMING_PROB_BASE;
    private static double OFFPEAK_INCOMING_PROB = OFFPEAK_INCOMING_PROB_BASE;

    // 固定车辆配置
    private static final List<String> FIXED_VEHICLE_PLATES = Arrays.asList("鄂AU657P", "鄂AH1B68");
    private static final int FIXED_VEHICLE_COLOR = 1; // 蓝色

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

    // 卡口数据类
    static class TollData {
        String plateNumber;
        int vehicleType;
        String uploadTime;
        String deviceId;
        int headLaneCode;
        int vehicleColor; // 新增车辆颜色
        double vehicleWeight; // 新增车辆重量

        public TollData(String plateNumber, int vehicleType, String uploadTime,
                        String deviceId, int headLaneCode, int vehicleColor, double vehicleWeight) {
            this.plateNumber = plateNumber;
            this.vehicleType = vehicleType;
            this.uploadTime = uploadTime;
            this.deviceId = deviceId;
            this.headLaneCode = headLaneCode;
            this.vehicleColor = vehicleColor;
            this.vehicleWeight = vehicleWeight;
        }
    }

    // 车辆数据类 - 新增颜色和重量属性
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
        String specialFlag; // 特殊标志字段
        int vehicleColor; // 车辆颜色 (0-6,9-12)
        double vehicleWeight; // 车辆重量 (吨)
        boolean isFixedVehicle; // 是否是固定车辆

        // 用于性能优化的缓存字段
        private transient int segmentIndex = -1;
        private transient boolean isOnBridge = false;

        public TData(int id, String carNumber, int vehicleType, double speed, int laneNo,
                     long mileage, int direction, Random random, boolean isFixedVehicle) {
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
            this.specialFlag = generateSpecialFlag(random); // 生成特殊标志
            this.vehicleColor = isFixedVehicle ? FIXED_VEHICLE_COLOR : generateVehicleColor(random); // 生成车辆颜色
            this.vehicleWeight = generateVehicleWeight(random); // 生成车辆重量
            this.isFixedVehicle = isFixedVehicle;
            updateCachedProperties();
        }

        public TData(TData original, int newWayno, long newTpointno, double newSpeed, boolean idChanged, int newDirection) {
            this.id = idChanged ? Math.abs(UUID.randomUUID().hashCode()) : original.id;
            this.plateNo = original.plateNo;
            this.vehicleType = original.vehicleType;
            this.speed = formatSpeed(newSpeed);
            this.laneNo = newWayno;
            this.mileage = newTpointno;
            this.direction = newDirection;
            this.stakeId = getStakeNumber();
            this.passedTollGates = new HashSet<>(original.passedTollGates);
            double[] lnglat = generateInitialJingWei(newTpointno, newDirection);
            this.longitude = lnglat[0];
            this.latitude = lnglat[1];
            this.specialFlag = original.specialFlag; // 复制特殊标志
            this.vehicleColor = original.vehicleColor; // 复制车辆颜色
            this.vehicleWeight = original.vehicleWeight; // 复制车辆重量
            this.isFixedVehicle = original.isFixedVehicle;
            updateCachedProperties();
        }

        // 生成特殊标志（90%为"0"，10%为0-30的随机数）
        private String generateSpecialFlag(Random random) {
            if (random.nextDouble() < 0.9) {
                return "0";
            } else {
                return String.valueOf(random.nextInt(31));
            }
        }

        // 生成车辆颜色 (0-6,9-12)
        private int generateVehicleColor(Random random) {
            // 创建包含0-6和9-12的列表
            List<Integer> colors = new ArrayList<>();
            for (int i = 0; i <= 6; i++) colors.add(i);
            for (int i = 9; i <= 12; i++) colors.add(i);

            // 随机选择一个颜色
            return colors.get(random.nextInt(colors.size()));
        }

        // 生成车辆重量 (1.0-30.0吨)
        private double generateVehicleWeight(Random random) {
            return Double.parseDouble(WEIGHT_FORMAT.format(1.0 + random.nextDouble() * 29.0));
        }

        private void updateCachedProperties() {
            // 计算分段索引
            for (int i = 0; i < SEGMENT_ENDS.length; i++) {
                if (mileage <= SEGMENT_ENDS[i]) {
                    segmentIndex = i;
                    break;
                }
            }
        }

        public String getStakeNumber() {
            long km = (mileage / 1000);
            long meter = (mileage % 1000);
            return String.format("K%d+%03d", km, meter);
        }

        private static double formatSpeed(double speed) {
            return Double.parseDouble(SPEED_FORMAT.format(speed));
        }

        @Override
        public String toString() {
            return "TData{" +
                    "plateNo='" + plateNo + '\'' +
                    ", speed=" + speed +
                    ", laneNo=" + laneNo +
                    ", mileage=" + mileage +
                    ", direction=" + direction +
                    ", color=" + vehicleColor +
                    ", weight=" + vehicleWeight +
                    ", isFixed=" + isFixedVehicle +
                    '}';
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

    // ======================= 核心方法 =======================
    public static void main(String[] args) throws InterruptedException {
        redirectOutputToFile();
        parseCommandLineArguments(args);
        System.out.println("优化版轨迹模拟程序启动");

        final long baseTime = System.currentTimeMillis();
        final int interval = 200;
        int sn = 1;
        Random mainRandom = new Random();
        List<TData> activeVehicles = new ArrayList<>();
        int dynamicAdjustCount = 0;

        // 添加两辆固定车辆
        activeVehicles.addAll(createFixedVehicles(mainRandom));

        while (!Thread.currentThread().isInterrupted()) {

            final long loopStartNano = System.nanoTime();
            final long targetTime = getTargetTime(sn, baseTime, interval);

            // 1. 更新车辆位置
            activeVehicles = updateVehiclePositions(activeVehicles, 0.2, mainRandom);

            // 解决碰撞问题
            activeVehicles = resolveCollisions(activeVehicles);

            // 2. 生成新车辆
            activeVehicles = generateIncomingVehicles(activeVehicles, mainRandom, targetTime);

            // 3. 检查数据点缺失（1%概率）
            if (mainRandom.nextDouble() < DATA_POINT_MISSING_PROBABILITY) {
                System.out.printf("SN%d: 数据点整体缺失（概率 %.2f）%n",
                        sn, DATA_POINT_MISSING_PROBABILITY);
                long durationMs = (System.nanoTime() - loopStartNano) / 1_000_000;
                monitorPerformance(durationMs, sn);
                if (sn % ADJUST_INTERVAL == 0) {
                    adjustIncomingProbability(activeVehicles.size(), durationMs);
                }
                long currentTime = System.currentTimeMillis();
                long sleepTime = (targetTime + interval) - currentTime;
                if (sleepTime > 0) {
                    Thread.sleep(sleepTime);
                }
                sn++;
                continue;
            }

            // 4. 创建原始数据
            FiberGratingJsonData originalData = new FiberGratingJsonData(sn, targetTime, activeVehicles);

            // 5. 处理特殊车辆
            List<TData> specialVehicles = activeVehicles.stream()
                    .filter(vehicle -> !"0".equals(vehicle.getSpecialFlag()))
                    .collect(Collectors.toList());

            if (!specialVehicles.isEmpty()) {
                FiberGratingJsonData specialData = new FiberGratingJsonData(
                        sn,
                        targetTime,
                        specialVehicles
                );
                producerSpecial.sendData(specialData);
            }

            // 6. 分段处理
            List<List<TData>> segmentVehicles = new ArrayList<>();
            for (int i = 0; i < 11; i++) segmentVehicles.add(new ArrayList<>());

            // 7. 卡口检测
            List<TollData> tollDataList = new ArrayList<>();

            // 8. 处理车辆数据
            for (TData vehicle : activeVehicles) {
                if (vehicle.segmentIndex >= 0 && vehicle.segmentIndex < 11) {
                    segmentVehicles.get(vehicle.segmentIndex).add(vehicle);
                }

                detectTollGates(vehicle, originalData.timeStamp, tollDataList);
            }

            // 9. 发送卡口数据
            if (!tollDataList.isEmpty()) {
                Gson gson = new Gson();
                String tollJson = gson.toJson(tollDataList);
                producerToll.sendData("tollData", String.valueOf(sn), tollJson);
            }

            // 10. 并行发送分段数据
            parallelProcessSegments(segmentVehicles, originalData);

            // 11. 性能监控和调整
            long durationMs = (System.nanoTime() - loopStartNano) / 1_000_000;
            monitorPerformance(durationMs, sn);
            if (sn % ADJUST_INTERVAL == 0) {
                adjustIncomingProbability(activeVehicles.size(), durationMs);
            }

            // 12. 计算并等待
            long currentTime = System.currentTimeMillis();
            long sleepTime = (targetTime + interval) - currentTime;

            if (sleepTime > 0) {
                Thread.sleep(sleepTime);
            } else {
                System.out.printf("⏱️ SN%d 延迟: %dms%n", sn, -sleepTime);
                dynamicAdjustCount++;
                applyDynamicAdjustment(-sleepTime, dynamicAdjustCount);
            }

            sn++;
        }

        // 关闭生产者
        for (KafkaProducerUtil producer : producers) {
            producer.close();
        }
        producerToll.close();
        producerSpecial.close();
    }

    // ======================= 辅助方法 =======================
    // 创建固定车辆
    private static List<TData> createFixedVehicles(Random random) {
        List<TData> fixedVehicles = new ArrayList<>();

        // 第一辆固定车辆
        fixedVehicles.add(new TData(
                Math.abs(UUID.randomUUID().hashCode()),
                "鄂AU657P",
                generateVehicleType(random),
                generateInitialSpeed(random),
                2, // 车道2
                MIN_MILEAGE,
                1, // 上行方向
                random,
                true // 是固定车辆
        ));

        // 第二辆固定车辆
        fixedVehicles.add(new TData(
                Math.abs(UUID.randomUUID().hashCode()),
                "鄂AH1B68",
                generateVehicleType(random),
                generateInitialSpeed(random),
                3, // 车道3
                MIN_MILEAGE,
                1, // 上行方向
                random,
                true // 是固定车辆
        ));

        return fixedVehicles;
    }

    // 生成符合要求的车辆类型（1-4或11-16）
    private static int generateVehicleType(Random random) {
        if (random.nextBoolean()) {
            return 1 + random.nextInt(4);
        } else {
            return 11 + random.nextInt(6);
        }
    }

    private static long getTargetTime(int sn, long baseTime, int interval) {
        return baseTime + sn * interval;
    }

    private static void monitorPerformance(long durationMs, int sn) {
        if (durationMs > WARNING_THRESHOLD_MS) {
            System.out.printf("⚠️ 警告 SN%d: 处理耗时 %dms%n", sn, durationMs);
        }
        if (durationMs > CRITICAL_THRESHOLD_MS) {
            System.out.printf("🚨 严重 SN%d: 处理耗时 %dms%n", sn, durationMs);
        }
    }

    private static void applyDynamicAdjustment(long delayMs, int adjustCount) {
        if (adjustCount > MAX_DYNAMIC_ADJUST) return;
        if (delayMs > CRITICAL_THRESHOLD_MS) {
            int newInterval = 200 + (int)(delayMs - CRITICAL_THRESHOLD_MS);
            System.out.printf("动态调整: 间隔从200ms增加到%dms%n", newInterval);
        }
    }

    private static void parallelProcessSegments(List<List<TData>> segmentVehicles,
                                                FiberGratingJsonData originalData) {
        int processors = Runtime.getRuntime().availableProcessors();
        ExecutorService executor = Executors.newFixedThreadPool(processors);

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

    private static long parseTimestamp(String timestamp) {
        try {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS");
            LocalDateTime localDateTime = LocalDateTime.parse(timestamp, formatter);
            return localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        } catch (Exception e) {
            return System.currentTimeMillis();
        }
    }

    private static void adjustIncomingProbability(int currentVehicleCount, long durationMs) {
        double densityFactor = Math.sqrt((double) currentVehicleCount / IDEAL_VEHICLE_COUNT);
        double newPeakProb = PEAK_INCOMING_PROB_BASE / densityFactor;
        double newOffPeakProb = OFFPEAK_INCOMING_PROB_BASE / densityFactor;

        PEAK_INCOMING_PROB = Math.max(MIN_PROB, Math.min(MAX_PROB, newPeakProb));
        OFFPEAK_INCOMING_PROB = Math.max(MIN_PROB, Math.min(MAX_PROB, newOffPeakProb));

        System.out.printf("智能调整: 车辆数=%d, 密度因子=%.2f, 新概率[高峰=%.4f, 平峰=%.4f], 处理时长=%dms%n",
                currentVehicleCount, densityFactor,
                PEAK_INCOMING_PROB, OFFPEAK_INCOMING_PROB,
                durationMs);

        if (durationMs > CRITICAL_THRESHOLD_MS) {
            PEAK_INCOMING_PROB *= 0.8;
            OFFPEAK_INCOMING_PROB *= 0.8;
            System.out.printf("🚨 超时降级: 概率额外降低20%% → [高峰=%.4f, 平峰=%.4f]%n",
                    PEAK_INCOMING_PROB, OFFPEAK_INCOMING_PROB);
        }
    }

    private static void detectTollGates(TData vehicle, String timestamp, List<TollData> tollDataList) {
        List<TollGate> relevantGates = (vehicle.direction == 1) ? UP_Toll_GATES : DOWN_Toll_GATES;

        for (TollGate gate : relevantGates) {
            if (vehicle.getPassedTollGates().contains(gate.id)) continue;

            long lowerBound = gate.mileage - 20;
            long upperBound = gate.mileage;

            if (vehicle.direction == 1) {
                if (vehicle.mileage >= lowerBound && vehicle.mileage <= upperBound) {
                    tollDataList.add(new TollData(
                            vehicle.plateNo,
                            vehicle.vehicleType,
                            formatToSecondPrecision(timestamp),
                            gate.id,
                            vehicle.laneNo,
                            vehicle.vehicleColor,
                            vehicle.vehicleWeight
                    ));
                    vehicle.getPassedTollGates().add(gate.id);
                }
            } else {
                if (vehicle.mileage <= upperBound && vehicle.mileage >= lowerBound) {
                    tollDataList.add(new TollData(
                            vehicle.plateNo,
                            vehicle.vehicleType,
                            formatToSecondPrecision(timestamp),
                            gate.id,
                            vehicle.laneNo,
                            vehicle.vehicleColor,
                            vehicle.vehicleWeight
                    ));
                    vehicle.getPassedTollGates().add(gate.id);
                }
            }
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
            vehicles.add(new TData(id, carNumber, generateVehicleType(random), speed, wayno, tpointno, direct, random, false));
        }
        return ensureSafeInitialPositions(vehicles);
    }

    private static double generateInitialSpeed(Random random) {
        return TData.formatSpeed(80 + random.nextDouble() * 40);
    }

    private static long generateInitialPosition(Random random) {
        return 1049000 + (long) (random.nextDouble() * 900);
    }

    private static double[] generateInitialJingWei(long tpointno, int direc) {
        TrafficEventUtils.MileageConverter converter = (direc == 1) ? mileageConverter1 : mileageConverter2;
        return converter.findCoordinate(tpointno).getLnglat();
    }

    // 核心错误数据生成方法
    private static List<TData> updateVehiclePositions(List<TData> vehicles, double elapsedTime,
                                                      Random random) {
        return vehicles.parallelStream()
                .map(vehicle -> {
                    // 为每个车辆创建独立的随机数生成器
                    Random vehicleRandom = new Random(random.nextLong());

                    // 1. 速度变化（20%概率）
                    double newSpeed = vehicle.speed;
                    if (vehicleRandom.nextDouble() < SPEED_CHANGE_PROBABILITY) {
                        double change = vehicleRandom.nextDouble() * 2 * SPEED_CHANGE_RANGE - SPEED_CHANGE_RANGE;
                        newSpeed = Math.max(80, Math.min(120, vehicle.speed * (1 + change)));
                    }

                    // 2. 位置更新
                    int directionFactor = (vehicle.direction == 1) ? 1 : -1;
                    long positionChange = (long) (newSpeed / 3.6 * elapsedTime * directionFactor);
                    long newTpointno = vehicle.mileage + positionChange;

                    // 3. 边界检查和固定车辆处理
                    if ((vehicle.direction == 1 && newTpointno > MAX_MILEAGE) ||
                            (vehicle.direction == 2 && newTpointno < MIN_MILEAGE)) {
                        // 如果是固定车辆，则调头
                        if (vehicle.isFixedVehicle) {
                            int newDirection = (vehicle.direction == 1) ? 2 : 1;
                            newTpointno = (vehicle.direction == 1) ? MAX_MILEAGE : MIN_MILEAGE;
                            return new TData(vehicle, vehicle.laneNo, newTpointno, newSpeed, false, newDirection);
                        } else {
                            return null;
                        }
                    }

                    // 4. 车道变化（10%概率）
                    boolean idChanged = false;
                    int newWayno = vehicle.laneNo;
                    if (elapsedTime > 5 && vehicleRandom.nextDouble() < WAYNO_CHANGE_PROBABILITY) {
                        int candidateWayno = newWayno + (vehicleRandom.nextBoolean() ? 1 : -1);
                        if (candidateWayno >= 1 && candidateWayno <= 4) {
                            newWayno = candidateWayno;
                        }
                    }
                    return new TData(vehicle, newWayno, newTpointno, newSpeed, false, vehicle.direction);
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    // 解决车辆碰撞问题
    private static List<TData> resolveCollisions(List<TData> vehicles) {
        Map<Integer, Map<Integer, List<TData>>> laneDirectionMap = new TreeMap<>();
        for (TData v : vehicles) {
            laneDirectionMap.computeIfAbsent(v.laneNo, k -> new HashMap<>())
                    .computeIfAbsent(v.direction, k -> new ArrayList<>())
                    .add(v);
        }

        List<TData> safeVehicles = new ArrayList<>();
        for (Map.Entry<Integer, Map<Integer, List<TData>>> laneEntry : laneDirectionMap.entrySet()) {
            for (Map.Entry<Integer, List<TData>> directionEntry : laneEntry.getValue().entrySet()) {
                List<TData> dirVehicles = directionEntry.getValue();
                dirVehicles.sort((v1, v2) ->
                        directionEntry.getKey() == 1 ?
                                Long.compare(v1.mileage, v2.mileage) :
                                Long.compare(v2.mileage, v1.mileage)
                );

                for (int i = 0; i < dirVehicles.size(); i++) {
                    TData current = dirVehicles.get(i);
                    if (i < dirVehicles.size() - 1) {
                        TData rearVehicle = dirVehicles.get(i + 1);
                        long distance = (directionEntry.getKey() == 1) ?
                                rearVehicle.mileage - current.mileage :
                                current.mileage - rearVehicle.mileage;

                        if (distance < MIN_SAFE_DISTANCE) {
                            long newRearPosition = (directionEntry.getKey() == 1) ?
                                    current.mileage + MIN_SAFE_DISTANCE :
                                    current.mileage - MIN_SAFE_DISTANCE;
                            rearVehicle = new TData(rearVehicle, rearVehicle.laneNo, newRearPosition, rearVehicle.speed, false, rearVehicle.direction);
                            dirVehicles.set(i + 1, rearVehicle);
                        }
                    }
                    safeVehicles.add(current);
                }
            }
        }
        return safeVehicles;
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
                    safeVehicles.add(new TData(vehicle, vehicle.laneNo, newPosition, vehicle.speed, false, vehicle.direction));
                    prevPosition = newPosition;
                }
            }
        }
        return safeVehicles;
    }

    // 生成新车辆
    private static List<TData> generateIncomingVehicles(List<TData> currentVehicles, Random random,
                                                        long timestamp) {
        List<TData> newVehicles = new ArrayList<>(currentVehicles);
        double incomingProb = isPeakTime(timestamp) ? PEAK_INCOMING_PROB : OFFPEAK_INCOMING_PROB;

        // 入口生成
        if (random.nextDouble() < incomingProb) {
            int lane = random.nextInt(4) + 1;
            if (isLaneSafeForEntry(currentVehicles, lane, 1, MIN_MILEAGE)) {
                newVehicles.add(createNewVehicle(lane, 1, random));
            }
        }

        if (random.nextDouble() < incomingProb) {
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
                generateVehicleType(random),
                generateInitialSpeed(random),
                lane,
                entryPosition,
                direction,
                random,
                false
        );
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

    private static void parseCommandLineArguments(String[] args) {
        if (args == null || args.length == 0) return;

        try {
            PEAK_INCOMING_PROB_BASE = Double.parseDouble(args[0]);
            if (args.length > 1) OFFPEAK_INCOMING_PROB_BASE = Double.parseDouble(args[1]);
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
        PEAK_INCOMING_PROB_BASE = 0.5 / 2;
        OFFPEAK_INCOMING_PROB_BASE = 0.15 / 2;
        PEAK_INCOMING_PROB = PEAK_INCOMING_PROB_BASE;
        OFFPEAK_INCOMING_PROB = OFFPEAK_INCOMING_PROB_BASE;
        PEAK_INITIAL_VEHICLES = 3;
        OFFPEAK_INITIAL_VEHICLES = 3;
        System.out.println("使用默认配置值");
    }

    // Kafka生产者工具类
    public static class KafkaProducerUtil implements AutoCloseable {
        private final String TOPIC;
        private final Producer<String, String> producer;
        private final Gson gson = new Gson();

        public KafkaProducerUtil(String topic) {
            this.TOPIC = topic;
            Properties props = new Properties();//100.65.38.40:9092
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.0.5:9092");
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
}