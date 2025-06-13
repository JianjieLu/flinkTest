package whu.edu.moniData;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.UUID;

import com.google.gson.Gson;
import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import whu.edu.moniData.Utils.TrafficEventUtils;

import java.util.Properties;


public class FiberGratingData2 {
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
    // 优化：真实车牌生成（中国车牌格式：省级简称+字母+5位数字/字母）
    private static final List<String> PROVINCE_CODES = Arrays.asList(
            "京", "沪", "津", "渝", "冀", "晋", "蒙", "辽", "吉", "黑",
            "苏", "浙", "皖", "闽", "赣", "鲁", "豫", "鄂", "湘", "粤",
            "桂", "琼", "川", "贵", "云", "藏", "陕", "甘", "青", "宁"
    );

    // 优化：第二位字母限制为A-H（模拟地级市代码常见范围）
    private static final String CITY_LETTERS = "ABCDEFGH"; // 仅A到H共8个字母
    private static final String UPPER_CASE_LETTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"; // 后五位字母仍保持全字母，但概率降低

    private static final long MIN_MILEAGE = 1016020;   // K1016+20（上行起点/下行终点）
//    private static final long MAX_MILEAGE = 1175545;   // K1175+545（上行终点/下行起点）
    private static final long MAX_MILEAGE = 1173790;

    // 各分段结束位置（含）
    private static final long[] SEGMENT_ENDS = {
            1037954,  // K1037+954
            1048271,  // K1048+271
            1068083,  // K1068+083
            1083768,  // K1083+768
            1099029,  // K1099+029（原数据可能有误，假设为029而非29）
            1115410,  // K1115+410
            1125320,  // K1125+320
            1140176,  // K1140+176
            1156422,  // K1156+422
            1166689,  // K1166+689
            1173790   // K1173+790
    };

//    // 里程分段配置
//    private static final long SEGMENT1_END = MIN_MILEAGE + 53175;   // 第一段结束位置（含）
//    private static final long SEGMENT2_END = SEGMENT1_END + 53175;  // 第二段结束位置（含）
//
//
//    private static final String[] TOPICS = {
//            "fiberDataTest1",  // 第一段Topic
//            "fiberDataTest2",  // 第二段Topic
//            "fiberDataTest3"   // 第三段Topic
//    };

    // 十一个Kafka主题
    private static final String[] TOPICS = new String[11];
    static {
        for (int i = 0; i < 11; i++) {
            TOPICS[i] = "fiberData" + (i + 1);
        }
    }

//    private static final KafkaProducerUtil producer1 = new KafkaProducerUtil(TOPICS[0]); // 分段1主题
//    private static final KafkaProducerUtil producer2 = new KafkaProducerUtil(TOPICS[1]); // 分段2主题
//    private static final KafkaProducerUtil producer3 = new KafkaProducerUtil(TOPICS[2]); // 第三段Topic
    // 十一个Producer实例（使用列表管理）
    private static final List<KafkaProducerUtil> producers = new ArrayList<>();
    static {
        for (String topic : TOPICS) {
            producers.add(new KafkaProducerUtil(topic));
        }
    }
    // -----------------------------------------------------------------------------

    // 新增：卡口主题与Producer实例
    private static final String TOLL_TOPIC = "tollData"; // 卡口专属主题
    private static final KafkaProducerUtil producerToll = new KafkaProducerUtil(TOLL_TOPIC); // 卡口Producer

    private static final DecimalFormat SPEED_FORMAT = new DecimalFormat("#.00");
    private static final double SPEED_CHANGE_PROBABILITY = 0.2; // 20%概率变化速度
    private static final double SPEED_CHANGE_RANGE = 0.1; // ±10%速度变化范围
    private static final double DATA_POINT_MISSING_PROBABILITY = 0.01; // 1%数据点整体缺失概率
    private static final double ID_CHANGE_PROBABILITY = 0.0001; // 0.01%车辆ID突变概率
    private static final double WAYNO_CHANGE_PROBABILITY = 0.1; // 10%车辆变道概率
    private static final int MIN_SAFE_DISTANCE = 10; // 最小安全距离10米
    private static final double NOISE_POINT_PROBABILITY = 0.001; // 0.1%概率生成噪声点（可调整）

//    private static final double INCOMING_VEHICLE_PROBABILITY = 0.2; // 20%概率生成新驶入车辆


    // ------------------- 新增：时间分段配置 -------------------
    private static final int PEAK_START_MORNING = 7;   // 早高峰开始时间（小时，24小时制）
    private static final int PEAK_END_MORNING = 9;    // 早高峰结束时间
    private static final int PEAK_START_EVENING = 17;  // 晚高峰开始时间
    private static final int PEAK_END_EVENING = 19;    // 晚高峰结束时间

    private static final double PEAK_INCOMING_PROB = 0.5 / 2;   // 高峰期车辆驶入概率（50%）
    private static final double OFFPEAK_INCOMING_PROB = 0.15 / 2; // 低谷期车辆驶入概率（15%）

    private static final int PEAK_INITIAL_VEHICLES = 100;   // 高峰期初始车辆数
    private static final int OFFPEAK_INITIAL_VEHICLES = 30;  // 低谷期初始车辆数

    // 桥梁范围配置（里程：米）
    private static final long BRIDGE_START = 1050000; // 桥起点（包含）
    private static final long BRIDGE_END = 1055000;   // 桥终点（包含）

    // ------------------- 卡口相关新增代码 -------------------
    // 卡口基础信息
    private static class TollGate {
        String id;            // 卡口ID（1-5上行，6-10下行）
        long mileage;      // 卡口位置（米）

        public TollGate(String id, long mileage) {
            this.id = id;
            this.mileage = mileage;
        }
    }

    // 上行卡口（根据 Excel 中 F 列值为 1 确定，ID 从 A 列取）
    private static final List<TollGate> UP_Toll_GATES = Arrays.asList(
            new TollGate("KKJK-02", 1030900),
            new TollGate("KKJK-04", 1033700),
            new TollGate("KKJK-06", 1043550),
            new TollGate("KKJK-08", 1058350),
            new TollGate("KKJK-10", 1062600),
            new TollGate("KKJK-12", 1063250),
            new TollGate("KKJK-14", 1075600),
            new TollGate("KKJK-15", 1086450),
            new TollGate("KKJK-18", 1092600),
            new TollGate("KKJK-20", 1110150),
            new TollGate("KKJK-22", 1112750),
            new TollGate("KKJK-25", 1115550),
            new TollGate("KKJK-26", 1116300),
            new TollGate("KKJK-28", 1122800),
            new TollGate("KKJK-30", 1129300),
            new TollGate("KKJK-33", 1140950),
            new TollGate("KKJK-35", 1146800),
            new TollGate("KKJK-38", 1149800),
            new TollGate("KKJK-40", 1154400),
            new TollGate("KKJK-42", 1162600),
            new TollGate("KKJK-43", 1163300),
            new TollGate("KKJK-45", 1165450),
            new TollGate("KKJK-47", 1168400),
            new TollGate("KKJK-49", 1173450)
//            new TollGate("KKJK-50", 1174200)
    );

    // 下行卡口（根据 Excel 中 F 列值为 2 确定，ID 从 A 列取）
    private static final List<TollGate> DOWN_Toll_GATES = Arrays.asList(
            new TollGate("KKJK-01", 1030000),
            new TollGate("KKJK-03", 1032900),
            new TollGate("KKJK-05", 1043200),
            new TollGate("KKJK-07", 1057800),
            new TollGate("KKJK-09", 1061800),
            new TollGate("KKJK-11", 1062850),
            new TollGate("KKJK-13", 1074820),
            new TollGate("KKJK-16", 1086730),
            new TollGate("KKJK-17", 1092150),
            new TollGate("KKJK-19", 1109600),
            new TollGate("KKJK-21", 1111780),
            new TollGate("KKJK-23", 1114750),
            new TollGate("KKJK-24", 1115500),
            new TollGate("KKJK-27", 1121900),
            new TollGate("KKJK-29", 1128800),
            new TollGate("KKJK-31", 1139900),
            new TollGate("KKJK-32", 1140400),
            new TollGate("KKJK-34", 1145700),
            new TollGate("KKJK-36", 1148064),
            new TollGate("KKJK-37", 1148600),
            new TollGate("KKJK-39", 1153400),
            new TollGate("KKJK-41", 1161500),
            new TollGate("KKJK-44", 1164560),
            new TollGate("KKJK-46", 1168000),
            new TollGate("KKJK-48", 1172890)
    );

    // 卡口记录数据类（按用户要求修改字段名和时间格式）
    static class TollData {
        String plateNumber;     // 车牌（原plateNo）
        int vehicleType;        // 车辆类型（0/1）
        String uploadTime;      // 记录时间（格式：yyyy-MM-dd HH:mm:ss，原recordTime）
        String deviceId;           // 卡口ID（原tollGateId）
        int headLaneCode;       // 车道号（原laneNo）

        public TollData(String plateNumber, int vehicleType, String uploadTime, String deviceId, int headLaneCode) {
            this.plateNumber = plateNumber;
            this.vehicleType = vehicleType;
            this.uploadTime = uploadTime;
            this.deviceId = deviceId;
            this.headLaneCode = headLaneCode;
        }

        @Override
        public String toString() {
            return "TollData{" +
                    "plateNumber='" + plateNumber + '\'' +
                    ", vehicleType=" + vehicleType +
                    ", uploadTime='" + uploadTime + '\'' +
                    ", deviceId=" + deviceId +
                    ", headLaneCode=" + headLaneCode +
                    '}';
        }
    }
    // ------------------- 卡口相关代码结束 -------------------
@Getter
@Setter
    static class TData {
        int id;
        String plateNo;
        int vehicleType;
        double speed;
        int laneNo; // 1-4车道
        long mileage; // 位置点（米）
        int direction; // 方向（1:上行/2:下行）
        // 新增：公开skateid的getter（供Gson序列化使用）
        @Getter
        String stakeId; // 新增：桩号字段
        @Getter
        private final Set<String> passedTollGates; // 新增：记录已通过的卡口ID

        double longitude;
        double latitude;
        // 构造函数 1（全参数初始化）
        public TData(int id, String carNumber, int vehicleType, double speed, int laneNo,
                     long mileage, int direction) {
            this.id = id;
            this.plateNo = carNumber;
            this.vehicleType = vehicleType;
            this.speed = formatSpeed(speed);
            this.laneNo = laneNo;
            this.mileage = mileage;
            this.direction = direction;
            this.stakeId = getStakeNumber(); // 初始化skateid（基于当前Mileage）
            this.passedTollGates = new HashSet<>(); // 初始化已通过卡口集合
            double[] lnglat = generateInitialJingWei(mileage, direction);
            this.longitude = lnglat[0];
            this.latitude = lnglat[1];
        }

        // 构造函数 2（基于原有车辆创建新状态）
        public TData(TData original, int newWayno, long newTpointno, double newSpeed, boolean idChanged) {
            this.id = idChanged ? Math.abs(UUID.randomUUID().hashCode()) : original.id;
            this.plateNo = original.plateNo;
            this.vehicleType = original.vehicleType;
            this.speed = formatSpeed(newSpeed);
            this.laneNo = newWayno;
            this.mileage = newTpointno; // 新里程
            this.direction = original.direction;
            this.stakeId = getStakeNumber(); // 基于新里程生成skateid
            this.passedTollGates = new HashSet<>(original.passedTollGates); // 复制已通过卡口状态
            double[] lnglat = generateInitialJingWei(newTpointno, original.direction);
            this.longitude = lnglat[0];
            this.latitude = lnglat[1];
        }

        // 获取桩号字符串（KXXX+XXX 格式）
        public String getStakeNumber() {
            long km = (mileage / 1000);
            long meter = (mileage % 1000);
            return String.format("K%d+%03d", km, meter);
        }

        // toString 方法（可选：如需打印skateid，可添加）
        @Override
        public String toString() {
            return "TData{" +
                    "ID=" + id +
                    ", Carnumber='" + plateNo + '\'' +
                    ", Type=" + vehicleType +
                    ", Speed=" + SPEED_FORMAT.format(speed) +
                    ", Wayno=" + laneNo +
                    ", Tpointno=" + mileage + "(" + getStakeNumber() + ")" +
                    ", Direct=" + direction +
                    ", skateid='" + stakeId + '\'' +
                    '}';
        }

        private static double formatSpeed(double speed) {
            return Double.parseDouble(SPEED_FORMAT.format(speed));
        }

    }

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

        // 时间格式调整为毫秒级
        public static String formatTimestamp(long timestamp) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
            return sdf.format(new Date(timestamp));
        }

        @Override
        public String toString() {
            DecimalFormat timeFormat = new DecimalFormat("000");
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            return "\n--- 数据条目 SN: " + SN + " ---" +
                    "\n时间戳: " + timeStamp +
                    "\n当前车辆数: " + pathNum +
                    (pathNum > 0 ? "\n车辆详情:\n" + pathList : "\n（无车辆信息，全部驶出高速）");
        }
    }

    private static String generateCarNumber(Random random) {
        // 1. 随机选择省级行政区简称（如：鄂、豫）
        String provinceCode = PROVINCE_CODES.get(random.nextInt(PROVINCE_CODES.size()));

        // 2. 第二位字母限制为A-H（地级市代码常见范围，如鄂A、豫B等）
        char cityLetter = CITY_LETTERS.charAt(random.nextInt(CITY_LETTERS.length())); // A-H之间

        // 3. 后5位：80%数字，20%字母（字母仍为全大写，A-Z）
        StringBuilder suffix = new StringBuilder();
        for (int i = 0; i < 5; i++) {
            if (random.nextDouble() < 0.8) { // 80%概率数字
                suffix.append(random.nextInt(10)); // 0-9
            } else { // 20%概率字母（A-Z）
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
            int wayno = random.nextInt(4) + 1; // 1-4车道
            long tpointno = generateInitialPosition(random); // 初始位置在MIN-MAX之间随机

            int direct = random.nextInt(2) + 1; // 随机方向1(上行)/2(下行)
            vehicles.add(new TData(id, carNumber, random.nextInt(10), speed, wayno, tpointno, direct));
        }
        return ensureSafeInitialPositions(vehicles); // 初始化时确保安全间距
    }

    private static double generateInitialSpeed(Random random) {
        double rawSpeed = 80 + random.nextDouble() * 40; // 初始速度80-120km/h
        return TData.formatSpeed(rawSpeed);
    }

    private static long generateInitialPosition(Random random) {
        // 在 [MIN_MILEAGE, MAX_MILEAGE] 范围内生成随机位置（包含边界）
        long range = MAX_MILEAGE - MIN_MILEAGE + 1;
        return MIN_MILEAGE + (long) (random.nextDouble() * range);
    }
    private static double[] generateInitialJingWei(long tpointno,int direc) {
        TrafficEventUtils.MileageConverter converter = (direc == 1) ? mileageConverter1 : mileageConverter2;
        return converter.findCoordinate((int) tpointno).getLnglat();
    }

    /**
     * 生成噪声车辆（凭空出现的异常数据点）
     * @param random 随机数生成器
     * @return 噪声车辆数据
     */
    private static TData generateNoiseVehicle(Random random) {
        long randomMileage = MIN_MILEAGE + (long) (random.nextDouble() * (MAX_MILEAGE - MIN_MILEAGE + 1));
        double randomSpeed = generateInitialSpeed(random); //
        int randomLane = random.nextInt(4) + 1; // 1-4车道
        int randomDirection = random.nextInt(2) + 1; // 随机方向
        String randomPlate = generateCarNumber(random); // 随机车牌
        int randomType = random.nextInt(10); // 随机车辆类型

        // 生成唯一ID（使用UUID确保唯一性）
        int noiseId = Math.abs(UUID.randomUUID().hashCode());

        return new TData(noiseId, randomPlate, randomType, randomSpeed, randomLane, randomMileage, randomDirection);
    }

    private static List<TData> updateVehiclePositions(List<TData> vehicles, double elapsedTime, Random random) {
        List<TData> updatedVehicles = new ArrayList<>();

        for (TData vehicle : vehicles) {
            // 1. 处理速度变化
            double newSpeed = vehicle.speed;
            if (random.nextDouble() < SPEED_CHANGE_PROBABILITY) {
                double change = random.nextDouble() * 2 * SPEED_CHANGE_RANGE - SPEED_CHANGE_RANGE;
                newSpeed = Math.max(80, Math.min(120, vehicle.speed * (1 + change))); // 速度限制80-120km/h
            }

            // 2. 计算新位置（米）：速度转换为m/s后乘以时间（秒），根据方向调整（上行+，下行-）
            int directionFactor = (vehicle.direction == 1) ? 1 : -1;
//            long newTpointno = (long) (vehicle.mileage + (newSpeed / 3.6 * elapsedTime * directionFactor));
            double timeIntervalSeconds = 0.2; // 固定间隔0.2秒（每秒5个数据点）
            long positionChange = (long) (newSpeed / 3.6 * timeIntervalSeconds * directionFactor);
            long newTpointno = vehicle.mileage + positionChange;

            // 3. 检查是否超出边界（上行不能超过MAX，下行不能低于MIN）
            if ((vehicle.direction == 1 && newTpointno > MAX_MILEAGE) ||
                    (vehicle.direction == 2 && newTpointno < MIN_MILEAGE)) {
                continue; // 超出边界则移除该车辆
            }

            // 4. 处理ID突变
            boolean idChanged = random.nextDouble() < ID_CHANGE_PROBABILITY;

            // 5. 处理变道逻辑（5秒后允许变道，使用旧位置检查安全性）
            int newWayno = vehicle.laneNo;
            if (elapsedTime > 5 && random.nextDouble() < WAYNO_CHANGE_PROBABILITY) {
                int candidateWayno = newWayno + (random.nextBoolean() ? 1 : -1); // 随机左右变道
                if (candidateWayno >= 1 && candidateWayno <= 4) { // 有效车道
                    // 检查目标车道同方向是否有近距离车辆（10米内），使用旧位置（未更新的位置）
                    if (!hasCloseVehicle(vehicles, candidateWayno, vehicle.mileage, vehicle.direction, vehicle)) {
                        newWayno = candidateWayno;
                    }
                }
            }

            // 创建新车辆对象并添加到列表
            updatedVehicles.add(new TData(vehicle, newWayno, newTpointno, newSpeed, idChanged));
        }

        // 6. 处理碰撞（按车道和方向分组，调整位置确保安全间距）
        return resolveCollisions(updatedVehicles);
    }

    private static boolean hasCloseVehicle(List<TData> vehicles, int lane, long position, int direction, TData currentVehicle) {
        return vehicles.stream()
                .anyMatch(v ->
                        v.laneNo == lane &&          // 同一车道
                                v.direction == direction &&    // 同一方向
                                v.id != currentVehicle.id && // 非自身车辆
                                Math.abs(v.mileage - position) < MIN_SAFE_DISTANCE
                );
    }

    private static List<TData> resolveCollisions(List<TData> vehicles) {
        // 1. 按车道和方向分组（车道升序，方向0/1）
        Map<Integer, Map<Integer, List<TData>>> laneDirectionMap = new TreeMap<>();
        for (TData v : vehicles) {
            laneDirectionMap.computeIfAbsent(v.laneNo, k -> new HashMap<>())
                    .computeIfAbsent(v.direction, k -> new ArrayList<>())
                    .add(v);
        }

        List<TData> safeVehicles = new ArrayList<>();

        // 2. 处理每个车道+方向的车辆组
        for (Map.Entry<Integer, Map<Integer, List<TData>>> laneEntry : laneDirectionMap.entrySet()) {
            int lane = laneEntry.getKey();
            for (Map.Entry<Integer, List<TData>> directionEntry : laneEntry.getValue().entrySet()) {
                int direction = directionEntry.getKey();
                List<TData> dirVehicles = directionEntry.getValue();

                // 按行驶方向排序：上行（Direct=1）位置升序（前小后大），下行（Direct=2）位置降序（前大后小）
                dirVehicles.sort((v1, v2) ->
                        direction == 1 ?
                                Long.compare(v1.mileage, v2.mileage) :
                                Long.compare(v2.mileage, v1.mileage)
                );

                // 3. 检查前后车距离并调整（仅调整后车位置，简化处理）
                for (int i = 0; i < dirVehicles.size(); i++) {
                    TData current = dirVehicles.get(i);

                    // 处理后车（同向同车道，后车位置需大于前车+安全距离）
                    if (i < dirVehicles.size() - 1) {
                        TData rearVehicle = dirVehicles.get(i + 1);
                        long distance = (direction == 1) ?
                                rearVehicle.mileage - current.mileage :  // 上行：后车位置 - 前车位置
                                current.mileage - rearVehicle.mileage;   // 下行：前车位置 - 后车位置

                        if (distance < MIN_SAFE_DISTANCE) {
                            // 后车调整到安全距离外（根据方向调整位置）
                            long newRearPosition = (direction == 1) ?
                                    current.mileage + MIN_SAFE_DISTANCE :
                                    current.mileage - MIN_SAFE_DISTANCE;

                            // 创建新的后车对象，避免修改原列表中的对象
                            rearVehicle = new TData(rearVehicle, rearVehicle.laneNo, newRearPosition, rearVehicle.speed, false);
                            dirVehicles.set(i + 1, rearVehicle); // 更新后车数据
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
            int lane = laneEntry.getKey();
            for (Map.Entry<Integer, List<TData>> directionEntry : laneEntry.getValue().entrySet()) {
                int direction = directionEntry.getKey();
                List<TData> dirVehicles = directionEntry.getValue();

                // 按方向排序初始车辆（上行升序，下行降序，确保同方向内位置有序）
                dirVehicles.sort((v1, v2) ->
                        direction == 1 ?
                                Long.compare(v1.mileage, v2.mileage) :
                                Long.compare(v2.mileage, v1.mileage)
                );

                // 初始化安全间距：上行从MIN_MILEAGE左侧开始，下行从MAX_MILEAGE右侧开始
                long prevPosition = (direction == 1) ? MIN_MILEAGE - MIN_SAFE_DISTANCE :  // 上行初始前边界外
                        MAX_MILEAGE + MIN_SAFE_DISTANCE;                // 下行初始前边界外

                for (TData vehicle : dirVehicles) {
                    long newPosition;
                    if (direction == 1) {
                        // 上行：新位置至少为前一辆车位置 + 安全距离，不超过MAX_MILEAGE
                        newPosition = Math.max(vehicle.mileage, prevPosition + MIN_SAFE_DISTANCE);
                        newPosition = Math.min(newPosition, MAX_MILEAGE);
                    } else {
                        // 下行：新位置至多为前一辆车位置 - 安全距离，不低于MIN_MILEAGE
                        newPosition = Math.min(vehicle.mileage, prevPosition - MIN_SAFE_DISTANCE);
                        newPosition = Math.max(newPosition, MIN_MILEAGE);
                    }
                    // 创建新车辆对象，确保初始位置安全
                    safeVehicles.add(new TData(vehicle, vehicle.laneNo, newPosition, vehicle.speed, false));
                    prevPosition = newPosition; // 更新前一辆车的位置
                }
            }
        }
        return safeVehicles;
    }

    private static List<TData> generateIncomingVehicles(List<TData> currentVehicles, Random random, long timestamp) {
        List<TData> newVehicles = new ArrayList<>(currentVehicles);
        double incomingProb = isPeakTime(timestamp) ? PEAK_INCOMING_PROB : OFFPEAK_INCOMING_PROB;

        // 处理上行车辆驶入（起点MIN_MILEAGE，方向1）
        if (random.nextDouble() < incomingProb) {
            int lane = random.nextInt(4) + 1;
            if (isLaneSafeForEntry(currentVehicles, lane, 1, MIN_MILEAGE)) {
                TData newVehicle = createNewVehicle(lane, 1, random);
                newVehicles.add(newVehicle);
            }
        }

        // 处理下行车辆驶入（起点MAX_MILEAGE，方向2）
        if (random.nextDouble() < incomingProb) {
            int lane = random.nextInt(4) + 1;
            if (isLaneSafeForEntry(currentVehicles, lane, 2, MAX_MILEAGE)) {
                TData newVehicle = createNewVehicle(lane, 2, random);
                newVehicles.add(newVehicle);
            }
        }

        return newVehicles;
    }

    private static boolean isLaneSafeForEntry(List<TData> vehicles, int lane, int direction, long entryPosition) {
        return vehicles.stream()
                .noneMatch(v -> v.laneNo == lane && v.direction == direction &&
                        Math.abs(v.mileage - entryPosition) < MIN_SAFE_DISTANCE);
    }

    private static TData createNewVehicle(int lane, int direction, Random random) {
        long entryPosition = (direction == 1) ? MIN_MILEAGE : MAX_MILEAGE; // 上行从起点进入，下行从终点进入
        int id = Math.abs(UUID.randomUUID().hashCode());
        String carNumber = generateCarNumber(random);
        double speed = generateInitialSpeed(random);
        return new TData(id, carNumber, random.nextInt(10), speed, lane, entryPosition, direction);
    }

    private static String formatToSecondPrecision(String timestampWithMs) {
        // 输入：yyyy-MM-dd HH:mm:ss:SSS，输出：yyyy-MM-dd HH:mm:ss
        return timestampWithMs.split(":")[0] + ":" + timestampWithMs.split(":")[1] + ":" + timestampWithMs.split(":")[2];
    }

    // 判断是否为高峰期（根据小时判断，简化逻辑）
    private static boolean isPeakTime(long timestamp) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(timestamp);
        int hour = calendar.get(Calendar.HOUR_OF_DAY);
        return (hour >= PEAK_START_MORNING && hour < PEAK_END_MORNING) ||
                (hour >= PEAK_START_EVENING && hour < PEAK_END_EVENING);
    }

    public static void main(String[] args) {
        long baseTime = System.currentTimeMillis();
        List<TData> activeVehicles = generateInitialVehicleData(new Random(), baseTime); // 初始车辆
        int sn = 1;


        while (true) { // 无限循环，持续生成数据
            long startTime = System.currentTimeMillis();

            // 更新车辆位置
            activeVehicles = updateVehiclePositions(activeVehicles, 0.2, new Random());
            activeVehicles = generateIncomingVehicles(activeVehicles, new Random(), startTime);

            // 生成噪声点（异常数据）
            Random random = new Random();
            if (random.nextDouble() < NOISE_POINT_PROBABILITY) {
                TData noiseVehicle = generateNoiseVehicle(random);
                activeVehicles.add(noiseVehicle);
                System.out.println("【噪声点】生成异常车辆: " + noiseVehicle.toString());
            }

            // 创建原始数据（包含所有车辆）
            long currentTime = baseTime + (long) (sn * 200); // 每条数据间隔200毫秒
            FiberGratingJsonData originalData = new FiberGratingJsonData(sn++, currentTime, activeVehicles);

            // 按十一段分段
            List<List<TData>> segmentVehicles = new ArrayList<>(11);
            for (int i = 0; i < 11; i++) {
                segmentVehicles.add(new ArrayList<>());
            }

            // 记录经过卡口的车辆
            List<TollData> tollDataList = new ArrayList<>();

            // 遍历所有车辆，进行分段和卡口检测
            for (TData vehicle : originalData.pathList) {
                long vehicleMileage = vehicle.mileage;
                int vehicleDirection = vehicle.direction;

                // ------------------- 新增：桥梁范围过滤 -------------------
                if (vehicleMileage >= BRIDGE_START && vehicleMileage <= BRIDGE_END) {
                    // 车辆在桥上，跳过光纤数据记录（不加入任何分段）
                    continue;
                }

                // 确定车辆所属分段（找到第一个大于当前里程的分段结束位置）
                int segmentIndex = 0;
                while (segmentIndex < 11 && vehicleMileage > SEGMENT_ENDS[segmentIndex]) {
                    segmentIndex++;
                }

                // 将车辆添加到对应分段（防御性检查：确保索引不越界）
                if (segmentIndex < 11) {
                    segmentVehicles.get(segmentIndex).add(vehicle);
                } else {
                    System.err.println("警告：车辆里程超出分段范围：" + vehicle.getStakeNumber());
                }

                // 卡口检测逻辑（保持原逻辑不变）
                if (vehicleDirection == 1) { // 上行
                    UP_Toll_GATES.forEach(gate -> {
                        if (vehicleMileage >= (gate.mileage - 20) && vehicleMileage <= gate.mileage &&
                                !vehicle.getPassedTollGates().contains(gate.id)) {
                            tollDataList.add(new TollData(
                                    vehicle.plateNo,
                                    vehicle.vehicleType,
                                    formatToSecondPrecision(originalData.timeStamp),
                                    gate.id,
                                    vehicle.laneNo
                            ));
                            vehicle.getPassedTollGates().add(gate.id);
                        }
                    });
                }

                if (vehicleDirection == 2) { // 下行
                    DOWN_Toll_GATES.forEach(gate -> {
                        if (vehicleMileage <= gate.mileage && vehicleMileage >= (gate.mileage - 20) &&
                                !vehicle.getPassedTollGates().contains(gate.id)) {
                            tollDataList.add(new TollData(
                                    vehicle.plateNo,
                                    vehicle.vehicleType,
                                    formatToSecondPrecision(originalData.timeStamp),
                                    gate.id,
                                    vehicle.laneNo
                            ));
                            vehicle.getPassedTollGates().add(gate.id);
                        }
                    });
                }
            }

            // 发送卡口数据到专属主题
            if (!tollDataList.isEmpty()) {
                Gson gson = new Gson();
                String tollJson = gson.toJson(tollDataList);
                producerToll.sendData("tollData", String.valueOf(originalData.SN), tollJson);
            }

            // 按分段发送数据到对应的Kafka主题
            for (int i = 0; i < 11; i++) {
                List<TData> segmentData = segmentVehicles.get(i);
                if (segmentData.isEmpty()) continue;

                // 时间戳处理（保持原逻辑不变）
                long timeObs;
                try {
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS");
                    LocalDateTime localDateTime = LocalDateTime.parse(originalData.timeStamp, formatter);
                    timeObs = localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
                } catch (Exception e) {
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SS");
                    LocalDateTime localDateTime = LocalDateTime.parse(originalData.timeStamp, formatter);
                    timeObs = localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
                }

                // 创建分段数据并发送
                FiberGratingJsonData segmentJsonData = new FiberGratingJsonData(
                        originalData.SN,
                        timeObs,
                        segmentData
                );

                // 使用对应的Producer发送到指定主题
                producers.get(i).sendData(segmentJsonData);
            }

            // 控制发送频率（确保每次循环间隔为200毫秒）
            long endTime = System.currentTimeMillis();
            long sleepTime = 200 - (endTime - startTime);
            if (sleepTime > 0) {
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        // 关闭所有Kafka生产者
        for (KafkaProducerUtil producer : producers) {
            producer.close();
        }
        producerToll.close();
    }


//    public static void main(String[] args) {
//        int initialVehicleCount = 50;
//        int duration = 60*10;
//        long baseTime = System.currentTimeMillis();
//
//        List<FiberGratingJsonData> dataList = generateFiberGratingData(initialVehicleCount, duration, baseTime);
//
//        for (FiberGratingJsonData originalData : dataList) {
//            List<List<TData>> segmentVehicles = new ArrayList<>(Arrays.asList(
//                    new ArrayList<>(),
//                    new ArrayList<>(),
//                    new ArrayList<>()
//            ));
//
//            List<TollData> tollDataList = new ArrayList<>();
//            for (TData vehicle : originalData.pathList) {
//                long vehicleMileage = vehicle.mileage;
//                int vehicleDirection = vehicle.direction;
//
//                // ------------------- 分段逻辑 -------------------
//                if (vehicleMileage <= SEGMENT1_END) {
//                    segmentVehicles.get(0).add(vehicle);
//                } else if (vehicleMileage <= SEGMENT2_END) {
//                    segmentVehicles.get(1).add(vehicle);
//                } else {
//                    segmentVehicles.get(2).add(vehicle);
//                }
//                // ------------------- 原有卡口逻辑 -------------------
//                if (vehicleDirection == 0) {
//                    UP_Toll_GATES.forEach(gate -> {
//                        if (vehicleMileage >= (gate.mileage - 20) && vehicleMileage <= gate.mileage &&
//                                !vehicle.getPassedTollGates().contains(gate.id)) {
//                            tollDataList.add(new TollData(
//                                    vehicle.plateNo,
//                                    vehicle.vehicleType,
//                                    formatToSecondPrecision(originalData.timeStamp), // 转换为秒级时间,
//                                    gate.id,
//                                    vehicle.laneNo
//                            ));
//                            vehicle.getPassedTollGates().add(gate.id);
//                        }
//                    });
//                }
//
//                if (vehicleDirection == 1) {
//                    DOWN_Toll_GATES.forEach(gate -> {
//                        if (vehicleMileage <= gate.mileage && vehicleMileage >= (gate.mileage - 20) &&
//                                !vehicle.getPassedTollGates().contains(gate.id)) {
//                            tollDataList.add(new TollData(
//                                    vehicle.plateNo,
//                                    vehicle.vehicleType,
//                                    formatToSecondPrecision(originalData.timeStamp), // 转换为秒级时间,
//                                    gate.id,
//                                    vehicle.laneNo
//                            ));
//                            vehicle.getPassedTollGates().add(gate.id);
//                        }
//                    });
//                }
//            }
//
//
//
//            // ------------------- 发送卡口数据到专属主题 -------------------
//            if (!tollDataList.isEmpty()) {
//                Gson gson = new Gson();
//                String tollJson = gson.toJson(tollDataList);
//                producerToll.sendData("tollDataTest", String.valueOf(originalData.SN), tollJson);
//            }
//
//
////            // ------------------- 光纤分段数据打印 -------------------
////            for (int i = 0; i < segmentVehicles.size(); i++) {
////                List<TData> segmentData = segmentVehicles.get(i);
////                if (segmentData.isEmpty()) continue;
////
////                System.out.println("\n--- 分段 " + (i + 1) + " 光纤数据 SN: " + originalData.SN + " ---");
////                System.out.println("时间戳: " + originalData.timeStamp);
////                System.out.println("车辆数: " + segmentData.size());
////                segmentData.forEach(vehicle -> System.out.println("  " + vehicle.toString()));
////            }
//
//            // ------------------- Kafka发送逻辑-------------------
//            for (int i = 0; i < segmentVehicles.size(); i++) {
//                List<TData> segmentData = segmentVehicles.get(i);
//                if (segmentData.isEmpty()) continue;
//                long timeObs;
//                try {
//                    // 尝试按三位毫秒格式解析
//                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS");
//                    LocalDateTime localDateTime = LocalDateTime.parse(originalData.timeStamp, formatter);
//                    timeObs = localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
//                } catch (Exception e) {
//                    // 若三位毫秒格式解析失败，尝试按两位毫秒格式解析
//                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SS");
//                    LocalDateTime localDateTime = LocalDateTime.parse(originalData.timeStamp, formatter);
//                    timeObs = localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
//                }
//
//                FiberGratingJsonData newData = new FiberGratingJsonData(
//                        originalData.SN,
//                        timeObs,
//                        segmentData
//                );
//
//                // 根据分段索引选择对应的Producer实例（保留原注释代码）
//                switch (i) {
//                    case 0:
//                        producer1.sendData(newData);
//                        break;
//                    case 1:
//                        producer2.sendData(newData);
//                        break;
//                    case 2:
//                        producer3.sendData(newData);
//                        break;
//                }
//            }
//            // ------------------- 保留Producer关闭逻辑（即使未发送数据）-------------------
//        }
//
//        // 保留Producer关闭逻辑
//        producer1.close();
//        producer2.close();
//        producer3.close();
//        producerToll.close();
//    }





    public static class KafkaProducerUtil implements AutoCloseable {
        private final String BOOTSTRAP_SERVERS = "100.65.38.40:9092";
        private final String TOPIC;
        private final Producer<String, String> producer;
        private final Gson gson = new Gson();

        public KafkaProducerUtil(String topic) {
            this.TOPIC = topic;
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.ACKS_CONFIG, "1");
            props.put(ProducerConfig.RETRIES_CONFIG, 3);

            this.producer = new KafkaProducer<>(props);
        }

        public void sendData(FiberGratingJsonData data) {
            String key = String.valueOf(data.SN);
            String value = gson.toJson(data);
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, key, value);
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    System.err.println("发送SN " + data.SN + " 失败: " + exception.getMessage());
                }
            });
        }

        // 发送卡口数据（JSON字符串，支持任意数据结构）
        public void sendData(String topic, String key, String jsonValue) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, jsonValue);
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    System.err.println("发送卡口数据失败: " + exception.getMessage());
                }
            });
        }


        @Override
        public void close() {
            producer.flush();
            producer.close();
        }
    }
}