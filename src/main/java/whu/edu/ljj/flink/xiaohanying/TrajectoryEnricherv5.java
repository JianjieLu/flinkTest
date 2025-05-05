package whu.edu.ljj.flink.xiaohanying;

import com.alibaba.fastjson2.JSON;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.*;

import static whu.edu.ljj.flink.xiaohanying.Utils.*;


/**
 * Special Edition v5 for 孝汉应
 * -> 延迟匹配所有有车牌号的数据，即使envState = 99（必须）
 * -> 额外引入上一次的车辆ID -> tempCarIdMap
 */
// 只是在不实际分区的情况下适用，因为到处都是调用函数，所以状态一定全进程共享
public class TrajectoryEnricherv5 extends CoProcessFunction<PathTData, GantryData, PathTData> implements Serializable{

    private long timeInterval;
    private GantryAssignment gantryAssign;

    TrajectoryEnricherv5(long timeInterval, GantryAssignment gantryAssign) {
        this.timeInterval = timeInterval;
        this.gantryAssign = gantryAssign;
    }

    StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.seconds(5))
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build();

    // 这其实可以设置定期检测上一次匹配到门架的时间，超时则删除整个
    // 真实作业环境中，目前考虑用Redis组为全局缓存层，当检测到某个CarId的车辆的轨迹输出时，则相应更新vehicleState
    StateTtlConfig vehiclettlConfig = StateTtlConfig
            .newBuilder(Time.seconds(80)) // 设置状态存活时间为 80 秒，此时间为保底时间
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite) // 每次写入时更新存活时间
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired) // 不返回过期数据
            .build();

    // 使用 MapState 缓存门架数据：Key 是门架数据的matchTime，Value 是门架数据
    private MapState<Long, Map<Integer, List<GantryData>>> gantryState;
    // 使用 MapState 缓存车牌匹配数据：Key 是门架数据的carID，Value 是匹配情况记录
    // 这个需要手动更新，在真实应用场景下，是要在redis缓存中全局更新，更新逻辑应该和“车辆轨迹表”相关联，当车辆轨迹表的轨迹输出时，代表该车已经驶离高速
    private MapState<Long, VehicleMapping> vehicleState;
    // 记录达成阈值的carId和车牌号，注意这里是单线程所以可以用HashMap，而且这里不可能记录到
    private Map<Long, String> fineMatchState;
    // 记录上次车的id，用于检测车辆id的连续性，保证效率所以用map
    private Map<Long, Integer> tempCarIdMap;
    // 无牌车的阈值要根据项目实际
    private final int NONE_PLATE_THRESHOLD = 5;
    // 有牌车的阈值要根据项目实际
    private final int PLATE_THRESHOLD = 10;
    // 防止车辆在同一卡口重复匹配
    private final long MIN_MATCH_INTERVAL = 15000;


    @Override
    public void open(Configuration parameters) {
        // 定义门架数据缓存状态（Key 是门架时间戳）
        MapStateDescriptor<Long, Map<Integer,List<GantryData>>> gantryDescriptor =
                new MapStateDescriptor<>("gantryState", Types.LONG, TypeInformation.of(new TypeHint<Map<Integer,List<GantryData>>>() {
                }));
        gantryDescriptor.enableTimeToLive(ttlConfig);
        gantryState = getRuntimeContext().getMapState(gantryDescriptor);

        MapStateDescriptor<Long, VehicleMapping> vehicleDescriptor =
                new MapStateDescriptor<>("vehicleState", Types.LONG, TypeInformation.of(VehicleMapping.class));
        vehicleDescriptor.enableTimeToLive(vehiclettlConfig);
        vehicleState = getRuntimeContext().getMapState(vehicleDescriptor);

        // 初始化fineMatch
        fineMatchState = new HashMap<>();

        // 初始化tempCarIdMap
        tempCarIdMap = new HashMap<>();
    }

    // 接收光栅数据，执行实时匹配
    @Override
    public void processElement1(PathTData traje, Context ctx, Collector<PathTData> out) throws Exception {

        updateVehicleState(traje);

        matchGantry(traje);

        for (PathPoint ppoint : traje.getPathList()) {
            if (fineMatchState.containsKey(ppoint.getId()))
                ppoint.setPlateNo(fineMatchState.get(ppoint.getId()));
            else if (Objects.equals(ppoint.getPlateNo(), ""))
                ppoint.setPlateNo(vehicleState.get(ppoint.getId()).getLastMMPlate());
        }

        tempCarIdMap.clear();
        for(PathPoint ppoint : traje.getPathList())
            tempCarIdMap.put(ppoint.getId(), 1);

        out.collect(traje);
    }

    // 处理并缓存门架数据
    @Override
    public void processElement2(GantryData gantry, Context ctx, Collector<PathTData> out) throws Exception {
        long recvGantryTs = System.currentTimeMillis();
        // 初步筛选门架数据
//            if(!gantry.getId().equals("2714AC28-984A-42E9-A001-36395F243E99"))
//                return;
        String plateNumber = gantry.getPlateNumber();
        if (plateNumber.equals("默A00000") &&
                gantry.getTollPlateNumber() != null)
            plateNumber = gantry.getPlateNumber();
        // fineMatchState不可能包含"默A00000"
        if (fineMatchState.containsKey(plateNumber))
            return;

        long gantryTimestamp = convertToTimestamp(gantry.getUploadTime());

        long matchTime;
        if (recvGantryTs / 1000 >= gantryTimestamp / 1000) {
            matchTime = (recvGantryTs / 1000) * 1000 + ((recvGantryTs % 1000) / 200) * 200 - timeInterval;
            gantry.setUploadTime(convertToTimestampString(recvGantryTs));
        }
        else
            matchTime = gantryTimestamp - timeInterval;

        Map<Integer, List<GantryData>> gantryBucket;
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

    private void updateVehicleState(PathTData traje) throws Exception {
        for (PathPoint ppoint : traje.getPathList()) {
            long id = ppoint.getId();
            if (vehicleState.contains(id))
                continue;
            else
                vehicleState.put(id, new VehicleMapping());
        }
    }

    private List<GantryData> strictPlateMacth(List<PathPoint> trajeList, List<GantryData> gantryList) throws Exception {
        Iterator<GantryData> iterator = gantryList.iterator();
        while (iterator.hasNext()) {
            GantryData gantry = iterator.next();
            // 首先过滤天气不好的情况
            // gantry.getJSONObject("params").getString("palteNumber") == "默A00000" && gantry.getJSONObject("params").getInteger(headLaneCode) == null
            if (!gantry.getEnvState().equals("99")) {
                for (PathPoint ppoint : trajeList) {
                    if (!Objects.equals(ppoint.getPlateNo(), "") ||
                            (ppoint.getDirection() == 2 && ppoint.getMileage() < gantry.getMileage()) ||
                            (ppoint.getDirection() == 1 && ppoint.getMileage() > gantry.getMileage()) ||
                            ppoint.getLaneNo() != gantry.getHeadLaneCode())
                        continue;
                    else {
                        setMacthedPlate(gantry, ppoint);
//                        System.out.println("\n匹配到了gantry：" + JSON.toJSONString(gantry));
                        // 删除匹配到的门架数据
                        iterator.remove();
                    }
                }
            }
        }
        return gantryList;
    }

    private List<GantryData> relaxedPlateMatch(List<PathPoint> trajeList, List<GantryData> gantryList) throws Exception {
        Iterator<GantryData> iterator = gantryList.iterator();
        List<PathPoint> suitPoints = new ArrayList<>();
        while (iterator.hasNext()) {
            GantryData gantry = iterator.next();
            if (!gantry.getEnvState().equals("99")) {
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
                if(suitPoints.size() == 1) {
                    PathPoint ppoint = suitPoints.get(0);
                    setMacthedPlate(gantry, ppoint);
//                    System.out.println("\n第二次匹配，匹配到了gantry：" + JSON.toJSONString(gantry));
                    // 删除匹配到的门架数据
                    iterator.remove();
                }
                else if(suitPoints.size() > 1) {
                    List<PathPoint> continuousPoints = new ArrayList<>(suitPoints);
                    for(PathPoint ppoint : suitPoints)
                        if(!tempCarIdMap.containsKey(ppoint.getId()))
                            continuousPoints.remove(ppoint);
                    // continuousPoints不空说明有连续的，则连续的车辆点优先，如果是空的，则说明此刻这里的点对应的车辆都是新出现的
                    if(!continuousPoints.isEmpty())
                        suitPoints = continuousPoints;
                    // 找到最合适的匹配点
                    // 目前感觉延迟出现的概率可能高一点
                    PathPoint ppoint;
                    ppoint = suitPoints.stream()
                            .filter(pathPoint -> pathPoint.getLaneNo() == gantry.getHeadLaneCode())
                            .min(Comparator.comparingInt(pathPoint -> Math.abs(pathPoint.getMileage() - gantry.getMileage())))
                            .orElse(null);

                    if(ppoint != null) {
                        setMacthedPlate(gantry, ppoint);
//                        System.out.println("\n第二次匹配，匹配到了gantry：" + JSON.toJSONString(gantry));
                        // 删除匹配到的门架数据
                        iterator.remove();
                    }
                    else {
//                        System.out.println("\n可能是出现了压线情况，下面进行纯扩距离匹配");
                        // 此时一定会有一个匹配结果
                        ppoint = suitPoints.stream()
                                .min(Comparator.comparingInt(pathPoint -> Math.abs(pathPoint.getMileage() - gantry.getMileage())))
                                .orElse(null);
                        setMacthedPlate(gantry, ppoint);
//                        System.out.println("\n第二次匹配，匹配到了gantry：" + JSON.toJSONString(gantry));
                        // 删除匹配到的门架数据
                        iterator.remove();
                    }
                }
            }
        }
        return gantryList;
    }

    private List<GantryData> lastPlateMacth(List<PathPoint> trajeList, List<GantryData> gantryList) throws Exception {
        Iterator<GantryData> iterator = gantryList.iterator();
        while (iterator.hasNext()) {
            GantryData gantry = iterator.next();
            // 仅限天气不好
            if (gantry.getEnvState().equals("99")) {
                List<PathPoint> suitPoints = new ArrayList<>();
                for(PathPoint ppoint : trajeList){
                    // 严格限制：已经匹配过的不参与匹配
                    if (!Objects.equals(ppoint.getPlateNo(), ""))
                        continue;
                    suitPoints.add(ppoint);
                }
                if(suitPoints.size() == 1) {
                    setMacthedPlate(gantry, suitPoints.get(0));
//                    System.out.println("\n最终匹配，匹配到了gantry：" + JSON.toJSONString(gantry));
                }
                else {
                    PathPoint finalMatchedPoint = suitPoints.stream()
                            .min(Comparator.comparingInt(pathPoint -> Math.abs(pathPoint.getMileage() - gantry.getMileage())))
                            .orElse(null);
                    setMacthedPlate(gantry, finalMatchedPoint);
                }
                // 删除匹配到的门架数据
                iterator.remove();
            }
        }
        return gantryList;
    }

    private void setMacthedPlate(GantryData gantry, PathPoint ppoint) throws Exception {
        String mactchedPlate;
        // 以tollRecord中的为准
        if(gantry.getTollPlateNumber() != null)
            mactchedPlate = gantry.getTollPlateNumber();
        else
            mactchedPlate = gantry.getPlateNumber();

        if(mactchedPlate.equals("默A00000")) {
            if(vehicleState.get(ppoint.getId()).getLastMMPlate().equals("") ||
                    vehicleState.get(ppoint.getId()).getLastMMPlate().equals(("默A00000"))) {
                ppoint.setPlateNo("默A00000");
                int defaultPlateSum = vehicleState.get(ppoint.getId()).getDefaultPlateSum();
                vehicleState.get(ppoint.getId()).setDefaultPlateSum(defaultPlateSum + 1);
                if(defaultPlateSum + 1 >= NONE_PLATE_THRESHOLD) {
                    vehicleState.remove(ppoint.getId());
                    fineMatchState.put(ppoint.getId(), "无牌车");
                    return;
                }
            }
            else {
                // 极端特殊情况，ETC中途未识别到
                ppoint.setPlateNo(vehicleState.get(ppoint.getId()).getLastMMPlate());
            }
        }
        else {
            Map<String, Integer> plateCounts = vehicleState.get(ppoint.getId()).getPlateCounts();
            if (plateCounts.containsKey(mactchedPlate))
                plateCounts.put(mactchedPlate, plateCounts.get(mactchedPlate) + 1);
            else
                plateCounts.put(mactchedPlate, 1);
            Pair<String, Integer> result = vehicleState.get(ppoint.getId()).getMostMatchedPlate();
            String mostMacthedPlate = result.getLeft();
            ppoint.setPlateNo(mostMacthedPlate);
            vehicleState.get(ppoint.getId()).setLastMMPlate(mostMacthedPlate);
            if(result.getRight() >= PLATE_THRESHOLD) {
                fineMatchState.put(ppoint.getId(), mactchedPlate);
                vehicleState.remove(ppoint.getId());
                return;
            }
        }
        vehicleState.get(ppoint.getId()).setLastUpdateTime(convertToTimestampMillis(ppoint.getTimeStamp()));
    }

    public void matchGantry(PathTData traje) throws Exception {
        long trajeTs = traje.getTime();
        if (!gantryState.isEmpty()) {
            if (gantryState.contains(trajeTs)) {
                if (!traje.getPathList().isEmpty()) {
                    Map<Integer, List<PathPoint>> sortedTDATA = new HashMap<>();
                    for (PathPoint ppoint : traje.getPathList()) {
                        if (fineMatchState.containsKey(ppoint.getId()))
                            continue;
                        if (trajeTs < vehicleState.get(ppoint.getId()).getLastUpdateTime() + MIN_MATCH_INTERVAL)
                            continue;
                        int matchMileage = gantryAssign.assignGantry(ppoint);
//                        System.out.println("matchMileage：" + matchMileage);
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
                    Map<Integer, List<GantryData>> gantryStateMap = gantryState.get(trajeTs);
                    for (Map.Entry<Integer, List<PathPoint>> ppointsEntry : sortedTDATA.entrySet()) {
                        int nowMacthMilegae = ppointsEntry.getKey();
                        List<PathPoint> nowTrajeList = ppointsEntry.getValue();
                        if (!gantryStateMap.containsKey(nowMacthMilegae)) {
//                            System.out.println("\n不合理现象出现，没有可以配的数据：" + JSON.toJSONString(ppointsEntry.getValue()));
//                            System.out.println("\n有可能是因为超过了lastUpdateTime + MIN_MATCH_INTERVAL，具体看一下，这里先继续匹配");
                            continue;
                        }
                        List<GantryData> gantryList = new ArrayList<>(gantryStateMap.get(ppointsEntry.getKey()));
                        // 执行初次匹配
                        List<GantryData> match1Remain = new ArrayList<>(strictPlateMacth(nowTrajeList, gantryList));
                        gantryStateMap.put(nowMacthMilegae, match1Remain);
                        if (!match1Remain.isEmpty()) {
                            // 二次匹配
                            List<GantryData> match2Remain = new ArrayList<>(relaxedPlateMatch(nowTrajeList, match1Remain));
                            gantryStateMap.put(nowMacthMilegae, match2Remain);
                            if (!match2Remain.isEmpty()) {
                                // 极端匹配
                                List<GantryData> match3Remain = new ArrayList<>(lastPlateMacth(nowTrajeList, match2Remain));
                                gantryStateMap.put(nowMacthMilegae, match3Remain);
                            }
                        }
                    }
                    for (Map.Entry<Integer, List<GantryData>> gantryEntry : gantryStateMap.entrySet()) {
                        if (gantryEntry.getValue().isEmpty())
                            continue;
                        for (GantryData gantry : gantryEntry.getValue()) {
                            // 针对个别延迟：至多延长4s匹配
//                            System.out.println("\n此时" + trajeTs + "匹配不上" + JSON.toJSONString(gantry) + " 正在延时匹配，最大延至：" + (convertToTimestamp(gantry.getUploadTime()) + 4000 - timeInterval));
                            if ((!gantry.getPlateNumber().equals("默A00000") || gantry.getTollPlateNumber() != null || !gantry.getEnvState().equals("99")) &&
                                    trajeTs + 400 <= convertToTimestamp(gantry.getUploadTime()) + 4000 - timeInterval) {
                                Map<Integer, List<GantryData>> nextGantryMap;
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
                                continue;
                            }
                        }
                    }
                } else {
                    Map<Integer, List<GantryData>> gantryStateMap = gantryState.get(trajeTs);
                    for (Map.Entry<Integer, List<GantryData>> gantryEntry : gantryStateMap.entrySet()) {
                        for (GantryData gantry : gantryEntry.getValue()) {
//                            System.out.println("\n此时" + trajeTs + "并没有光栅车辆轨迹点!" + JSON.toJSONString(gantry) + " 正在延时匹配，最大延至：" + (convertToTimestamp(gantry.getUploadTime()) + 4000 - timeInterval));
                            // 针对个别延迟：至多延长4s匹配
                            if ((!gantry.getPlateNumber().equals("默A00000") || gantry.getTollPlateNumber() != null || !gantry.getEnvState().equals("99")) &&
                                    trajeTs + 400 <= convertToTimestamp(gantry.getUploadTime()) + 4000 - timeInterval) {
                                Map<Integer, List<GantryData>> nextGantryMap;
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
                        }
                    }
                }
            }
        }
        // 前提是光栅数据相比于门架数据有天然的延迟，目前基本都大于1400ms
        gantryState.remove(trajeTs);
    }
}
