package whu.edu.moniData;

import whu.edu.moniData.Utils.TrafficEventUtils;

import java.io.IOException;
import java.util.Arrays;

import static whu.edu.moniData.predictFlinkMergeNoWriteWithTime.stakeToMileage;

public class test {
    public static void main(String[] args) throws IOException {
//        TrafficEventUtils.MileageConverter mileageConverter1=new TrafficEventUtils.MileageConverter("sx_json.json");
//        System.out.println(Arrays.toString(mileageConverter1.findCoordinate(stakeToMileage("K1017+67")).getLnglat()));
//        System.out.println(209715200*3);
        long l1= 1747998000000L;
        long l2= 1748001600000L;
        System.out.println(l2);
        System.out.println(l1-l2);
    }
}
