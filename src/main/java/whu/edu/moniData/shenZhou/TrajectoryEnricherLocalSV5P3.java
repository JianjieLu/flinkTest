package whu.edu.moniData.shenZhou;

import com.alibaba.fastjson2.JSON;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

import static whu.edu.moniData.shenZhou.Utils.*;

/**
 * Special Edition new-Local for simulation
 * 6.21
 *  1.批量操作版本
 * 7.8
 *  1.每隔十分钟清理一次，初始是20分钟
 *  2.将读取和写入Redis解耦，这里把之前的测流输出GantryRecord换成了RedisCacheUpdate
 *  3.设置Redis数据过期时间 //注：这里其实什么都没有改，设置过期时间仅在TGCTime中修改了
 */
// 只是在不实际分区的情况下适用，因为到处都是调用函数，所以状态一定全进程共享
public class TrajectoryEnricherLocalSV5P3 extends KeyedCoProcessFunction<Integer, PathTData, GantryData, PathTData> implements Serializable{

    private static final Logger LOG = LoggerFactory.getLogger(TrajectoryEnricherLocalSV5P3.class);
    private long timeInterval;
    private boolean isTimeInitialized;
    private transient GantryAssignment gantryAssign;
    private boolean isSetClearTimer;

    // 使用 MapState 缓存门架数据：Key 是门架数据的matchTime，Value 是门架数据
    private MapState<Long, Map<Double, List<GantryData>>> gantryState;
    // 使用 MapState 缓存车牌匹配数据：Key 是门架数据的carID，Value 是匹配情况记录
    // 这个需要手动更新，在真实应用场景下，是要在redis缓存中全局更新，更新逻辑应该和“车辆轨迹表”相关联，当车辆轨迹表的轨迹输出时，代表该车已经驶离高速
    private MapState<String, VehicleMapping> vehicleState;
    /*记录达成阈值的carId和车牌号
        5.10
         a.改成MapState
     */
    private MapState<String, VehicleInfo> fineMatchState;
    // 记录上次车的id，用于检测车辆id的连续性，保证效率所以用map <- 这个应该不会存在有卡口刚好卡着衔接处的
    private MapState<Long, Integer> tempCarIdMap;
    // 无牌车的阈值要根据项目实际 -> 目前不使用
//    private final int NONE_PLATE_THRESHOLD = 5;
    // 有牌车的阈值要根据项目实际
    private final int PLATE_THRESHOLD = 10;
    // 防止车辆在同一卡口重复匹配
    private final long MIN_MATCH_INTERVAL = 15000;
    // 最大延迟时间
    private final int MAX_DELAY_TIME = 6000;
    // jedis连接池
    private transient JedisPool jedisPool;


    @Override
    public void open(Configuration parameters) throws IOException {
        // 这里光栅时间演延长了是因为车辆要先缓存才能匹配
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.hours(2))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();

        // 这其实可以设置定期检测上一次匹配到门架的时间，超时则删除整个
        // 真实作业环境中，目前考虑用Redis组为全局缓存层，当检测到某个CarId的车辆的轨迹输出时，则相应更新vehicleState
        StateTtlConfig vehiclettlConfig = StateTtlConfig
                .newBuilder(Time.hours(2)) // 设置状态存活时间为 80 秒，此时间为保底时间
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite) // 每次写入时更新存活时间
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired) // 不返回过期数据
                .build();

        // 定义门架数据缓存状态（Key 是门架时间戳）
        MapStateDescriptor<Long, Map<Double,List<GantryData>>> gantryDescriptor =
                new MapStateDescriptor<>("gantryState", Types.LONG, TypeInformation.of(new TypeHint<Map<Double,List<GantryData>>>() {
                }));
        gantryDescriptor.enableTimeToLive(ttlConfig);
        gantryState = getRuntimeContext().getMapState(gantryDescriptor);

        MapStateDescriptor<String, VehicleMapping> vehicleDescriptor =
                new MapStateDescriptor<>("vehicleState", Types.STRING, TypeInformation.of(VehicleMapping.class));
        vehicleDescriptor.enableTimeToLive(vehiclettlConfig);
        vehicleState = getRuntimeContext().getMapState(vehicleDescriptor);

        // 初始化fineMatch
        MapStateDescriptor<String, VehicleInfo> fineMatchDescriptor =
                new MapStateDescriptor<>("fineMatchState", Types.STRING, TypeInformation.of(VehicleInfo.class));
        fineMatchState = getRuntimeContext().getMapState(fineMatchDescriptor);

        // 初始化tempCarIdMap
        MapStateDescriptor<Long, Integer> tempCarIdDescriptor =
                new MapStateDescriptor<>("tempCarIdState", Types.LONG, Types.INT);
        tempCarIdMap = getRuntimeContext().getMapState(tempCarIdDescriptor);

        // 初始化gantryAssign
        String gantryFilePath = "jgaGantry.xlsx";
        gantryAssign = new GantryAssignment(gantryFilePath);

        jedisPool = JedisPoolUtil.getJedisPoolInstance("100.65.38.141", 6380);
        try (Jedis jedis = jedisPool.getResource()) {
            String ping = jedis.ping(); // 返回"PONG"表示连接正常
            System.out.println("Redis连接测试: " + ping);
        } catch (Exception e) {
            System.err.println("连接池初始化失败: " + e.getMessage());
        }

        isTimeInitialized = false;

        isSetClearTimer = false;
    }

    // 接收光栅数据，执行实时匹配
    @Override
    public void processElement1(PathTData traje, Context ctx, Collector<PathTData> out) throws Exception {
        /*
            5.9 这里为了保持动态提取timeInterval，每次来新数据都会专门算一下
         */
        // 排序后的光栅数据进来了

        if(!isSetClearTimer) {
            long nextTriggerTime = System.currentTimeMillis() / (60*1000) * (60*1000) + 20*60*1000;
            ctx.timerService().registerProcessingTimeTimer(nextTriggerTime);
            isSetClearTimer = true;
//            System.out.println("现在时间："+System.currentTimeMillis()+", 设定20分钟后："+nextTriggerTime+", 进行第一次过期数据清理");
        }

        long beforeMatchTime = System.currentTimeMillis();

        if(!isTimeInitialized)
            isTimeInitialized = true;

        timeInterval = (200 - (traje.getTime() % 1000 % 200)) % 200;

//        int segId = ctx.getCurrentKey();
        int segId = traje.getSegId();
        // 根据车辆的光栅id和整段数据的segId更新vehicleState
        updateVehicleState(traje, segId);

        // 车牌匹配，重新定义测流输出
        // 7.3 这里或许可以获得匹配值然后使用Redis与flink的api作为数据流写入
        // 7.8 这里实现了匹配后更新结果测流输出，写入通过Redis与flink的api实现
        matchGantry(traje, ctx);

//        if(gantryRecord != null && gantryRecord.getAllGantrySum() != 0)
//        {
//            OutputTag<GantryRecord> outputTag = new OutputTag<GantryRecord>("allGantries") {};
//            ctx.output(outputTag, gantryRecord);
//        }

        // 依据历史记录补全此刻没有参与匹配的车辆轨迹点
        for (PathPoint ppoint : traje.getPathList()) {
            if (fineMatchState.contains("fm" + ppoint.getId()))
                ppoint.setPlateNo(fineMatchState.get("fm" + ppoint.getId()).getPlateNo());
            else if (Objects.equals(ppoint.getPlateNo(), "")) {
                VehicleMapping vehicleMapping = vehicleState.get("vs" + ppoint.getId());
                if(vehicleMapping == null)
                    System.out.println("莫名的错误，算子内的fm和vsState均无车辆："+ppoint.getId());
                else {
                    ppoint.setPlateNo(vehicleState.get("vs" + ppoint.getId()).getLastMMPlate());
                    ppoint.setPlateColor(vehicleState.get("vs" + ppoint.getId()).getPlateColor());
                    ppoint.setVehicleType(vehicleState.get("vs" + ppoint.getId()).getVehicleType());
                    ppoint.setOriginalType(vehicleState.get("vs" + ppoint.getId()).getOriginType());
                }
            }
        }

        // 这里只是存一下上一次的，表示这辆车之前出现过就行
        tempCarIdMap.clear();
        for(PathPoint ppoint : traje.getPathList())
            tempCarIdMap.put(ppoint.getId(), 1);

        // 遵从标准数据规范，补全timeStamp字段
        traje.setTimeStamp(convertFromTimestampMillis(traje.getTime()));

//        System.out.println("此次是路段 "+traje.getSegId()+"，此段路车辆轨迹点数："+traje.getPathNum()+"，此次匹配的花费时间："+(System.currentTimeMillis() - beforeMatchTime));

        out.collect(traje);
    }

    // 处理并缓存门架数据
    @Override
    public void processElement2(GantryData gantry, Context ctx, Collector<PathTData> out) throws Exception {
        if(!isTimeInitialized) {
            System.out.println("仍在初始化阶段");
            return;
        }

//        long recvGantryTs = System.currentTimeMillis();
        // 初步筛选门架数据
//            if(!gantry.getId().equals("2714AC28-984A-42E9-A001-36395F243E99"))
//                return;
//        String plateNumber;
//        if (gantry.getTollPlateNumber() != null)
//            plateNumber = gantry.getTollPlateNumber();
//        else
//            plateNumber = gantry.getPlateNumber();
        String plateNumber = gantry.getPlateNumber();
        // fineMatchState不可能包含"默A00000"，因为全都改成“无牌车”了
        if (!plateNumber.equals("默A00000")) {
            for(VehicleInfo vehicleInfo : fineMatchState.values())
                if (plateNumber.equals(vehicleInfo.getPlateNo()))
                    return;
        }

        long gantryTimestamp = convertToTimestamp(gantry.getUploadTime());

        long matchTime;
//        if (recvGantryTs / 1000 >= gantryTimestamp / 1000) {
//            matchTime = (recvGantryTs / 1000) * 1000 + ((recvGantryTs % 1000) / 200) * 200 - timeInterval;
//            gantry.setUploadTime(convertToTimestampString(recvGantryTs));
//        }
//        else
//            matchTime = gantryTimestamp - timeInterval;
        matchTime = gantryTimestamp - timeInterval;
//        System.out.println("\n这条gantry数据的matchTime为："+matchTime+"\n对应的timeInterval为："+timeInterval);

        Map<Double, List<GantryData>> gantryBucket;
        List<GantryData> gantryList;
        if (!gantryState.contains(matchTime))
            gantryBucket = new HashMap<>();
        else
            gantryBucket = gantryState.get(matchTime);
        if(!gantryBucket.containsKey(gantry.getMileage()))
            gantryList = new ArrayList<>();
        else
            gantryList = gantryState.get(matchTime).get(gantry.getMileage());

        // 将数据添加到桶中
        gantryList.add(gantry);
        gantryBucket.put(gantry.getMileage(), gantryList);
        gantryState.put(matchTime, gantryBucket);
    }

    private void updateVehicleState(PathTData traje, int segId) throws Exception {
        // 这里会自动关闭连接
        try (Jedis jedis = jedisPool.getResource()) {
            // 1. 收集所有需要查询的key
            List<String> vsKeysToQuery = new ArrayList<>();
            List<String> fmKeysToQuery = new ArrayList<>();
            List<Long> allKeys = new ArrayList<>();

            Map<String, PathPoint> ppointMap = new HashMap<>();
            Map<String, VehicleMapping> vtRedisData = new HashMap<>();
            Map<String, VehicleInfo> fRedisData = new HashMap<>();

            // 2. 初步更新，并获得vsKeysToQuery
            for (PathPoint ppoint : traje.getPathList()) {
                allKeys.add(ppoint.getId());
                if (ppoint.getDirection() == 1) {
                    if (segId == 1) {

                        long id = ppoint.getId();
                        if (vehicleState.contains("vs" + id))
                            continue;
                        else
                            vehicleState.put("vs" + id, new VehicleMapping());

                    } else if (segId >= 2 && segId <= 5) {

                        long id = ppoint.getId();
                        if (vehicleState.contains("vs" + id))
                            continue;
                        else {
                            vsKeysToQuery.add("vs" + id);
                            ppointMap.put("vs" + id, ppoint);
                        }

                    } else {

                        long id = ppoint.getId();
                        if (fineMatchState.contains("fm" + id))
                            continue;
                        if (vehicleState.contains("vs" + id))
                            continue;
                        else {
                            vsKeysToQuery.add("vs" + id);
                            ppointMap.put("vs" + id, ppoint);
                        }
                    }
                } else if (ppoint.getDirection() == 2) {
                    if (segId == 11) {

                        long id = ppoint.getId();
                        if (vehicleState.contains("vs" + id))
                            continue;
                        else
                            vehicleState.put("vs" + id, new VehicleMapping());

                    } else if (segId <= 10 && segId >= 8) {

                        long id = ppoint.getId();
                        if (vehicleState.contains("vs" + id))
                            continue;
                        else {
                            vsKeysToQuery.add("vs" + id);
                            ppointMap.put("vs" + id, ppoint);
                        }

                    } else {

                        long id = ppoint.getId();
                        if (fineMatchState.contains("fm" + id))
                            continue;
                        if (vehicleState.contains("vs" + id))
                            continue;
                        else {
                            vsKeysToQuery.add("vs" + id);
                            ppointMap.put("vs" + id, ppoint);
                        }
                    }
                }
            }
//            System.out.println("allKeys的条数:"+allKeys.size()+"，此条数据全部keys为："+allKeys);
//            System.out.println("vsKeys的条数:"+vsKeysToQuery.size()+"，待查询的vsKeys为："+vsKeysToQuery);
            // 3. vtKeysQuery
            if(!vsKeysToQuery.isEmpty()) {
                List<String> vtValues = jedis.mget(vsKeysToQuery.toArray(new String[0]));

                for (int i = 0; i < vsKeysToQuery.size(); i++) {
                    if (!Objects.equals(vtValues.get(i), null)) {
                        vtRedisData.put(vsKeysToQuery.get(i), JSON.parseObject(vtValues.get(i), VehicleMapping.class));
                    } else {
                        // 获得fKey
                        fmKeysToQuery.add("fm" + vsKeysToQuery.get(i).substring(2));
                    }
                }

                // 进行update，并且获得在vt阶段删除的id
                List<String> delVSId = new ArrayList<>();
                for (Map.Entry<String, VehicleMapping> entry : vtRedisData.entrySet()) {
                    String id = entry.getKey();
                    VehicleMapping vehicleMapping = entry.getValue();
                    PathPoint ppoint = ppointMap.get(id);
                    if ((ppoint.getDirection() == 1 && segId == 11) || (ppoint.getDirection() == 2 && segId == 1))
                        delVSId.add(id);
                    vehicleState.put(id, vehicleMapping);
                }
                // 批量删除vtKeys操作
                if (!delVSId.isEmpty()) {
                    jedis.del(delVSId.toArray(new String[0]));
                    System.out.println("两端车辆的vs缓存数据被删除："+delVSId);
                }
            }

            // 4.fKeysQuery
            List<String> newVSId = new ArrayList<>();
            if(!fmKeysToQuery.isEmpty()) {
                List<String> fValues = jedis.mget(fmKeysToQuery.toArray(new String[0]));
                for (int i = 0; i < fmKeysToQuery.size(); i++) {
                    if (!Objects.equals(fValues.get(i), null)) {
                        fRedisData.put(fmKeysToQuery.get(i), JSON.parseObject(fValues.get(i), VehicleInfo.class));
                    } else {
                        // 获得创建的新vsId
                        newVSId.add("vs" + fmKeysToQuery.get(i).substring(2));
                    }
                }

                // 进行update，并且获得在f阶段删除的id
                List<String> delFId = new ArrayList<>();
                for (Map.Entry<String, VehicleInfo> entry : fRedisData.entrySet()) {
                    String fmId = entry.getKey();
                    VehicleInfo vehicleInfo = entry.getValue();
                    String vsId = "vs" + fmId.substring(2);
                    PathPoint ppoint = ppointMap.get(vsId);
                    if (ppoint == null) {
                        System.err.println("Warning: ppoint not found for vsId=" + vsId);
                        LOG.warn("ppoint not found for vsId={}", vsId);
                        continue; // 跳过当前条目
                    }
                    if ((ppoint.getDirection() == 1 && segId == 11) || (ppoint.getDirection() == 2 && segId == 1))
                        delFId.add(fmId);
                    fineMatchState.put(fmId, vehicleInfo);
                }
                // 批量删除fKeys操作
                if (!delFId.isEmpty()) {
                    jedis.del(delFId.toArray(new String[0]));
                    System.out.println("两端车辆的fm缓存数据被删除："+delFId);
                }
            }
            // 5. 执行vehicleState的新kv创建
//            System.out.println("newVSId的个数："+newVSId.size());
            for(String vsId : newVSId)
                vehicleState.put(vsId, new VehicleMapping());

        } // 无论是否异常都会调用 jedis.close()
    }

    private List<GantryData> strictPlateMatch(List<PathPoint> trajeList, List<GantryData> gantryList, Context ctx) throws Exception {
        Iterator<GantryData> iterator = gantryList.iterator();
        List<PathPoint> suitPoints = new ArrayList<>();
        while (iterator.hasNext()) {
            GantryData gantry = iterator.next();
            // 去掉判断envState条件语句
            for (PathPoint ppoint : trajeList) {
                if (!Objects.equals(ppoint.getPlateNo(), "") ||
                        (ppoint.getDirection() == 2 && ppoint.getMileage() < gantry.getMileage()) ||
                        (ppoint.getDirection() == 1 && ppoint.getMileage() > gantry.getMileage()) ||
                        ppoint.getLaneNo() != gantry.getHeadLaneCode())
                    continue;
                else
                    suitPoints.add(ppoint);
            }
            PathPoint matchPoint;

            if (!suitPoints.isEmpty()) {
                matchPoint = suitPoints.stream()
                        .min(Comparator.comparingDouble(pathPoint -> Math.abs(pathPoint.getMileage() - gantry.getMileage())))
                        .orElse(null);
                setMatchedPlate(gantry, matchPoint, ctx);
//                System.out.println("\n点："+matchPoint.getId()+"-匹配到了gantry：" + JSON.toJSONString(gantry));
                // 删除匹配到的门架数据
                iterator.remove();
            }

        }
        return gantryList;
    }

    private List<GantryData> relaxedPlateMatch(List<PathPoint> trajeList, List<GantryData> gantryList, Context ctx) throws Exception {
        Iterator<GantryData> iterator = gantryList.iterator();
        List<PathPoint> suitPoints = new ArrayList<>();
        while (iterator.hasNext()) {
            GantryData gantry = iterator.next();

            for (PathPoint ppoint : trajeList) {
                // 严格限制：已经匹配过的不参与匹配
                if (!Objects.equals(ppoint.getPlateNo(), ""))
                    continue;
                // 放宽条件1：车道允许相邻（如压线行驶）
                // 发现在刚进合流车道到就会被拍到的情况，所以有特殊情况，laneNo == 5，但是因为设置了延迟匹配，所以目前还是不加
//                        boolean laneTolerance = Math.abs(ppoint.getLaneNo() - gantry.getHeadLaneCode()) <= 1;
                boolean laneTolerance = (Math.abs(ppoint.getLaneNo() - gantry.getHeadLaneCode()) <= 1 ||
                        ppoint.getLaneNo() == 5);

                // 放宽条件2：这里对于距离不做限制，因为前面已经筛选过了
//                        boolean mileageTolerance = Math.abs(ppoint.getMileage() - gantry.getMileage()) <= 50;

                // 这里在正式匹配车牌之前应该先确定具体匹配哪一个点，有可能此时刻车辆很多
                if (laneTolerance)
                    suitPoints.add(ppoint);
            }
            if (suitPoints.isEmpty())
                return gantryList;
            else if (suitPoints.size() == 1) {
                PathPoint ppoint = suitPoints.get(0);
                setMatchedPlate(gantry, ppoint, ctx);
//                System.out.println("\n第二次匹配，匹配到了gantry：" + JSON.toJSONString(gantry));
                // 删除匹配到的门架数据
                iterator.remove();
            } else {
                List<PathPoint> continuousPoints = new ArrayList<>(suitPoints);
                for (PathPoint ppoint : suitPoints)
                    if (!tempCarIdMap.contains(ppoint.getId()))
                        continuousPoints.remove(ppoint);
                // continuousPoints不空说明有连续的，则连续的车辆点优先，如果是空的，则说明此刻这里的点对应的车辆都是新出现的
                if (!continuousPoints.isEmpty())
                    suitPoints = continuousPoints;
                // 找到最合适的匹配点
                // 目前感觉延迟出现的概率可能高一点
                PathPoint ppoint;
                ppoint = suitPoints.stream()
                        .filter(pathPoint -> pathPoint.getLaneNo() == gantry.getHeadLaneCode())
                        .min(Comparator.comparingDouble(pathPoint -> Math.abs(pathPoint.getMileage() - gantry.getMileage())))
                        .orElse(null);

                if (ppoint != null) {
                    setMatchedPlate(gantry, ppoint, ctx);
//                    System.out.println("\n第二次匹配，匹配到了gantry：" + JSON.toJSONString(gantry));
                    // 删除匹配到的门架数据
                    iterator.remove();
                } else {
//                    System.out.println("\n可能是出现了压线情况，下面进行纯扩距离匹配");
                    // 此时一定会有一个匹配结果
                    ppoint = suitPoints.stream()
                            .min(Comparator.comparingDouble(pathPoint -> Math.abs(pathPoint.getMileage() - gantry.getMileage())))
                            .orElse(null);
                    setMatchedPlate(gantry, ppoint, ctx);
//                    System.out.println("\n第二次匹配，匹配到了gantry：" + JSON.toJSONString(gantry));
                    // 删除匹配到的门架数据
                    iterator.remove();
                }
            }

        }
        return gantryList;
    }

    private void setMatchedPlate(GantryData gantry, PathPoint ppoint, Context ctx) throws Exception {
        int segId = gantry.getSegId();
        Long vehicleId = ppoint.getId();
        OutputTag<Tuple2<String, String>> vs = new OutputTag<>("vs", Types.TUPLE(Types.STRING, Types.STRING));
        OutputTag<Tuple2<String, String>> fm = new OutputTag<>("fm", Types.TUPLE(Types.STRING, Types.STRING));

        if(gantry.getTollPlateColor() != null) {
            ppoint.setPlateColor(gantry.getTollPlateColor());
            // 暂时匹配车种
            ppoint.setOriginalType(gantry.getTollVehicleUserType());
            ppoint.setVehicleType(gantry.getTollFeeVehicleType());
        }
        else
            ppoint.setPlateColor(gantry.getPlateColor());
        // 永远更新至上一次的状态，且目前没有carColor，所以保留null
        if(!vehicleState.contains("vs"+vehicleId)) {
//            System.out.println("追踪错误，已经匹配到gantry了，但是vehicleState里没有对应记录，车辆ID为："+vehicleId);
            vehicleState.put("vs"+vehicleId, new VehicleMapping());
//            System.out.println("这里已补充创建");
        }
        vehicleState.get("vs"+vehicleId).setVehicleType(ppoint.getVehicleType());
        vehicleState.get("vs"+vehicleId).setPlateColor(ppoint.getPlateColor());
        vehicleState.get("vs"+vehicleId).setOriginType(ppoint.getOriginalType());

        String mactchedPlate;
        // 以tollRecord中的为准
//        if(gantry.getTollPlateNumber() != null)
//            mactchedPlate = gantry.getTollPlateNumber();
//        else
//            mactchedPlate = gantry.getPlateNumber();
        mactchedPlate = gantry.getPlateNumber();

        Map<String, Integer> plateCounts = vehicleState.get("vs"+vehicleId).getPlateCounts();
        if (plateCounts.containsKey(mactchedPlate))
            plateCounts.put(mactchedPlate, plateCounts.get(mactchedPlate) + 1);
        else
            plateCounts.put(mactchedPlate, 1);
        Pair<String, Integer> result = vehicleState.get("vs"+vehicleId).getMostMatchedPlate();
        String mostMatchedPlate = result.getLeft();
        ppoint.setPlateNo(mostMatchedPlate);
        vehicleState.get("vs"+vehicleId).setLastMMPlate(mostMatchedPlate);
        vehicleState.get("vs"+vehicleId).setLastUpdateTime(convertToTimestampMillis(ppoint.getTimeStamp()));
        Tuple2<String, String> matchResult = new Tuple2<>();

        if (result.getRight() >= PLATE_THRESHOLD) {
            if (mactchedPlate == "默A00000")
                fineMatchState.put("fm" + vehicleId, new VehicleInfo("无牌车", ppoint.getPlateColor(), ppoint.getVehicleType(), ppoint.getOriginalType(), ppoint.getOriginalColor(), ppoint.getSpecialFlag(), convertToTimestampMillis(ppoint.getTimeStamp())));
            else
                fineMatchState.put("fm" + vehicleId, new VehicleInfo(mactchedPlate, ppoint.getPlateColor(), ppoint.getVehicleType(), ppoint.getOriginalType(), ppoint.getOriginalColor(), ppoint.getSpecialFlag(), convertToTimestampMillis(ppoint.getTimeStamp())));
            vehicleState.remove("vs" + vehicleId);

            // 如果不在最后一段，则向Redis更新
            if (!(ppoint.getDirection() == 1 && segId == 11) && !(ppoint.getDirection() == 2 && segId == 1)) {
                matchResult.setFields("fm" + vehicleId, JSON.toJSONString(fineMatchState.get("fm" + vehicleId)));
                ctx.output(fm, matchResult);
//                System.out.println(System.currentTimeMillis()+", fm"+vehicleId+", fm测流输出");
            }
        }
        // 如果不在最后一段，则向Redis更新
        else if (!(ppoint.getDirection() == 1 && segId == 11) && !(ppoint.getDirection() == 2 && segId == 1)) {
            matchResult.setFields("vs" + vehicleId, JSON.toJSONString(vehicleState.get("vs" + vehicleId)));
            ctx.output(vs, matchResult);
//            System.out.println(System.currentTimeMillis()+", vs"+vehicleId+", vs测流输出");
        }
    }

    public RedisCacheUpdate matchGantry(PathTData traje, Context ctx) throws Exception {
        RedisCacheUpdate rUpdate;
//        List<GantryData> misGantries = new ArrayList<>();

        long trajeTs = traje.getTime();
        if (!gantryState.isEmpty()) {
            if (gantryState.contains(trajeTs)) {
                rUpdate = new RedisCacheUpdate();
//                rUpdate.setUploadTime(convertToTimestampString(trajeTs));
                if (!traje.getPathList().isEmpty()) {
                    Map<Double, List<PathPoint>> sortedTDATA = new HashMap<>();
                    for (PathPoint ppoint : traje.getPathList()) {
                        if (fineMatchState.contains("fm"+ppoint.getId()))
                            continue;
                        if (trajeTs < vehicleState.get("vs"+ppoint.getId()).getLastUpdateTime() + MIN_MATCH_INTERVAL)
                            continue;
                        double matchMileage = gantryAssign.assignGantry(ppoint);
//                        System.out.println("matchMileage：" + matchMileage);
                        // 注意看，其实这里并没有算入不在卡口匹配范围内的轨迹点
                        if (matchMileage == 0)
                            continue;
                        else {
                            List<PathPoint> ppointList;
                            if (!sortedTDATA.containsKey(matchMileage))
                                ppointList = new ArrayList<>();
                            else
                                ppointList = sortedTDATA.get(matchMileage);
                            ppointList.add(ppoint);
                            sortedTDATA.put(matchMileage, ppointList);
                        }
                    }
                    Map<Double, List<GantryData>> gantryStateMap = gantryState.get(trajeTs);

//                    // 统计现有的gantry数量
//                    // 放在这里是因为会出现这种情况：此时路上有车，pathTData里有轨迹点，但是没有在gantry附近的
//                    for(List<GantryData> gantryList : gantryStateMap.values())
//                        gantryRecord.setAllGantrySum(gantryRecord.getAllGantrySum() + gantryList.size());

                    for (Map.Entry<Double, List<PathPoint>> ppointsEntry : sortedTDATA.entrySet()) {
                        double nowMatchMilegae = ppointsEntry.getKey();
                        List<PathPoint> nowTrajeList = ppointsEntry.getValue();

                        if (!gantryStateMap.containsKey(nowMatchMilegae)) {
//                            System.out.println("\n不合理现象出现，匹配卡口位置："+nowMatchMilegae+"处，没有可以配的数据：" + JSON.toJSONString(ppointsEntry.getValue()));
//                            System.out.println("有可能是因为超过了lastUpdateTime + MIN_MATCH_INTERVAL，具体看一下，这里先继续匹配");
                            // 5.7 这里出现这个原因也有可能就是这个范围内确实有车辆点可以用于匹配，但是没有（这个方向的）这个卡口的的卡口数据
                            // 注意：虽然这里光栅数据会有3s的buffer，但是匹配顺序是不变的，即依然会反应出卡口数据到达的时间和顺序！！！=> 所以出现这个问题，很有可能就是数据太晚了
//                            System.out.println("也可能是没有对应的卡口数据，有可能是卡口数据太晚或者丢失了。");
                            continue;
                        }
                        List<GantryData> gantryList = new ArrayList<>(gantryStateMap.get(nowMatchMilegae)); // 这里改成了nowMatchMilegae，应该可以

                        // 执行初次匹配
                        List<GantryData> match1Remain = new ArrayList<>(strictPlateMatch(nowTrajeList, gantryList, ctx));
                        gantryStateMap.put(nowMatchMilegae, match1Remain);
                        if (!match1Remain.isEmpty()) {
                            // 二次匹配
                            List<GantryData> match2Remain = new ArrayList<>(relaxedPlateMatch(nowTrajeList, match1Remain, ctx));
                            gantryStateMap.put(nowMatchMilegae, match2Remain);
//                            if (!match2Remain.isEmpty()) {
//                                // 极端匹配
//                                List<GantryData> match3Remain = new ArrayList<>(lastPlateMatch(nowTrajeList, match2Remain));
//                                gantryStateMap.put(nowMatchMilegae, match3Remain);
//                            }
                        }
                    }
                    for (Map.Entry<Double, List<GantryData>> gantryEntry : gantryStateMap.entrySet()) {
                        if (gantryEntry.getValue().isEmpty())
                            continue;
                        for (GantryData gantry : gantryEntry.getValue()) {

                            /* 针对个别延迟：至多延长6s匹配
                                5.13
                                a. 注意这里修改了判断条件，去掉了envState和tollPlateNumber
                             */
//                            System.out.println("\n此时" + trajeTs + "匹配不上" + JSON.toJSONString(gantry) + " 正在延时匹配，最大延至：" + (convertToTimestamp(gantry.getUploadTime()) + MAX_DELAY_TIME - timeInterval));
                            if (!gantry.getPlateNumber().equals("默A00000") &&
                                    trajeTs + 400 <= convertToTimestamp(gantry.getUploadTime()) + MAX_DELAY_TIME - timeInterval) {

//                                // 延迟代表这个gantry数据不属于此刻了
//                                gantryRecord.setAllGantrySum(gantryRecord.getAllGantrySum() - 1);

                                Map<Double, List<GantryData>> nextGantryMap;
                                List<GantryData> nextGantryList;
                                if (!gantryState.contains(trajeTs + 400))
                                    nextGantryMap = new HashMap<>();
                                else
                                    nextGantryMap = gantryState.get(trajeTs + 400);
                                if (!nextGantryMap.containsKey(gantry.getMileage()))
                                    nextGantryList = new ArrayList<>();
                                else
                                    nextGantryList = nextGantryMap.get(gantry.getMileage());
                                nextGantryList.add(gantry);
                                nextGantryMap.put(gantry.getMileage(), nextGantryList);
                                gantryState.put(trajeTs + 400, nextGantryMap);
                            }
//                            else {
//                                misGantries.add(gantry);
//                                // 针对延时也匹配不到的gantry记录最后的时间
//                                gantryRecord.setAllGantrySum(gantryRecord.getAllGantrySum() + 1);
//                            }
                        }
                    }
                } else {
                    Map<Double, List<GantryData>> gantryStateMap = gantryState.get(trajeTs);
                    for (Map.Entry<Double, List<GantryData>> gantryEntry : gantryStateMap.entrySet()) {
                        for (GantryData gantry : gantryEntry.getValue()) {

                            /* 针对个别延迟：至多延长6s匹配
                                5.13
                                a. 注意这里修改了判断条件，去掉了envState和tollPlateNumber
                             */
//                            System.out.println("\n此时" + trajeTs + "并没有光栅车辆轨迹点!" + JSON.toJSONString(gantry) + " 正在延时匹配，最大延至：" + (convertToTimestamp(gantry.getUploadTime()) + MAX_DELAY_TIME - timeInterval));
                            // 针对个别延迟：至多延长6s匹配
                            if (!gantry.getPlateNumber().equals("默A00000") &&
                                    trajeTs + 400 <= convertToTimestamp(gantry.getUploadTime()) + MAX_DELAY_TIME - timeInterval) {

                                // 这里特别注意一个事，因为到这里我并没有汇总卡口数目，所以不需要再减去1

                                Map<Double, List<GantryData>> nextGantryMap;
                                List<GantryData> nextGantryList;
                                if (!gantryState.contains(trajeTs + 400))
                                    nextGantryMap = new HashMap<>();
                                else
                                    nextGantryMap = gantryState.get(trajeTs + 400);
                                if (!nextGantryMap.containsKey(gantry.getMileage()))
                                    nextGantryList = new ArrayList<>();
                                else
                                    nextGantryList = nextGantryMap.get(gantry.getMileage());
                                nextGantryList.add(gantry);
                                nextGantryMap.put(gantry.getMileage(), nextGantryList);
                                gantryState.put(trajeTs + 400, nextGantryMap);
                            }
//                            else {
//                                misGantries.add(gantry);
//                                // 针对延时也匹配不到的gantry记录最后的时间
//                                gantryRecord.setAllGantrySum(gantryRecord.getAllGantrySum() + 1);
//                            }
                        }
                    }
                }
//                gantryRecord.setMisGantries(misGantries);
//                gantryRecord.setAnomalyGantrySum(misGantries.size());
                // 前提是光栅数据相比于门架数据有天然的延迟，目前基本都大于1400ms
                gantryState.remove(trajeTs);
                return rUpdate;
            }
        }
        // 没有对应的光栅数据，直接返回null
        return null;
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<PathTData> out) throws Exception {
        Long beforeCleanTime = System.currentTimeMillis();
        // 1. 执行清理逻辑
        // 卡口数据实际上是实时的，这里最大允许10分钟的延迟，但这是非常极端的情况
        Iterator<Long> gantryIter = gantryState.keys().iterator();
        while (gantryIter.hasNext()) {
            Long matchTime = gantryIter.next();
            if (timestamp - matchTime > 10 * 60 * 1000) {
                gantryIter.remove(); // 使用迭代器删除
            }
        }

        // vehicleState这里支持最低匀速80km/h
        Iterator<Map.Entry<String, VehicleMapping>> vehicleIter = vehicleState.iterator();
        while (vehicleIter.hasNext()) {
            Map.Entry<String, VehicleMapping> entry = vehicleIter.next();
            if (timestamp - entry.getValue().getLastUpdateTime() > 15 * 60 * 1000) {
                vehicleIter.remove(); // 安全删除
            }
        }

        // fineMatch会保留30分钟，模拟最低速度36km/h的拥堵
        Iterator<Map.Entry<String, VehicleInfo>> fineMatchIter = fineMatchState.iterator();
        while (fineMatchIter.hasNext()) {
            Map.Entry<String, VehicleInfo> entry = fineMatchIter.next();
            if (timestamp - entry.getValue().getFineMatchTime() > 30 * 60 * 1000) {
                fineMatchIter.remove();
            }
        }

        // 2. 注册下一次触发（需新事件到来后重新注册）
        long nextTriggerTime = timestamp + 10 * 60 * 1000;
        ctx.timerService().registerProcessingTimeTimer(nextTriggerTime);

        // 3. 可选：输出日志或侧输出流
//        System.out.println("此次清理花费的时间："+(System.currentTimeMillis()-beforeCleanTime));
//        System.out.println("定时清理完成，下次触发时间: " + convertFromTimestampMillis(nextTriggerTime));
    }
}