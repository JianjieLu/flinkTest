package whu.edu.ljj.flink.utils;

import javafx.util.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static java.lang.Math.abs;
import static whu.edu.ljj.flink.xiaohanying.Utils.Location;

public class LocationOPJiQun {
    //latitude,longitude,哪个匝道
    public static Pair<Location, Location> UseLLGetSK(double target1, double target2, String whichK) throws IOException {
        List<Location> roadDataList = JsonReader.readJsonFile("/home/ljj/data/" + whichK + "_locations.json");
        // 精度、纬度 差值  第几个
        List<Pair<Pair<Location, Double>, Integer>> targets = new ArrayList<>();

        int i = 0;
        Location d = null;
        Location e = null;

        // 取出经度差值最小的十条数据
        for (Location l : roadDataList) {
            double diff = Math.abs(l.getLatitude() - target1);
            if (i < 10) {
                targets.add(new Pair<>(new Pair<>(l, diff), i));
                i++;
            }else {
                // 只有当新的数据比当前最大的差值更小时，才替换并保持排序
                if (diff < targets.get(9).getKey().getValue()) {
                    targets.set(9, new Pair<>(new Pair<>(l, diff), i));
                    targets.sort(Comparator.comparing(p -> p.getKey().getValue())); // 重新排序
                }
                i++;
            }
        }
        double minDifference = 300;
        for (i = 0; i < 10; i++) {
            // 计算总差值
            double temp = targets.get(i).getKey().getValue() + Math.abs(target2 - targets.get(i).getKey().getKey().getLongitude());
            if (temp < minDifference) {
                d = targets.get(i).getKey().getKey();
                e=roadDataList.get(targets.get(i).getValue()+1);
                minDifference = temp;
            }
        }

        return new Pair<>(d, e);
    }
    public static Location findClosest(List<Location> sortedData, double target) {
        int low = 0;
        int high = sortedData.size() - 1;
        int index = -1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            double midVal = sortedData.get(mid).getLocationNum();
            if (midVal < target) {
                low = mid + 1;
            } else if (midVal > target) {
                high = mid - 1;
            } else {
                index = mid;
                break;
            }
        }

        if (index != -1) {
            return sortedData.get(index);
        } else {
            if (low == 0) {
                return sortedData.get(0);
            } else if (low == sortedData.size()) {
                return sortedData.get(sortedData.size() - 1);
            } else {
                Location before = sortedData.get(low - 1);
                Location after = sortedData.get(low);
                return (abs(before.getLocationNum() - target) <= abs(after.getLocationNum() - target)) ? before : after;
            }
        }
    }
    public static Location getLocation(String jsonPath,double target) throws IOException {
        List<Location> roadDataList = JsonReader.readJsonFile(jsonPath);
        return findClosest(roadDataList,target);
    }
    public static Location UseSKgetLL(String sk,String whichK) throws IOException {
        List<Location> roadDataList = JsonReader.readJsonFile("/home/ljj/data/"+whichK+"_locations.json");
        for(Location l:roadDataList){
            if(l.getLocation()==sk)return l;
        }
        return null;
    }

    public static void main(String[] args) throws IOException {
        long currentTime = System.currentTimeMillis();

        for(int i = 0 ; i<10000 ; i ++){
            UseLLGetSK(30.916303634643555,114.04553985595704,"AK");
        }
        long endTime = System.currentTimeMillis();
        System.out.println("start:"+currentTime+"   end:"+endTime+"   cha:"+(endTime-currentTime));

//        System.out.println(UseLLGetSK(30.916303634643555,114.04553985595704,"AK"));
    }
    public static double getLongitude(String jsonPath,double target) throws IOException {
        List<Location> roadDataList = JsonReader.readJsonFile(jsonPath);
        return findClosest(roadDataList,target).getLongitude();
    }
    public static double getLatitude(String jsonPath,double target) throws IOException {
        List<Location> roadDataList = JsonReader.readJsonFile(jsonPath);
        return findClosest(roadDataList,target).getLatitude();
    }
//
//    public static void main(String[] args) throws IOException {
//        System.out.println(getLocation("D:\\learn\\codes\\a_idea_codes\\flinkTest\\src\\main\\java\\whu\\edu\\data\\孝汉应.json",1124447.0));
//    }

}

