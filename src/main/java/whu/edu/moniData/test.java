package whu.edu.moniData;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class test {

    public static void main(String[] args) {
        long st =1754818185150L / 1000 * 1000;
        long tt =1754818185150L / 1000 * 1000+1000;
        System.out.println("饱和度："+Math.round(mainSau(2200 * 8 * (tt - st) / 3600000.0,497) * 100.0) / 100.0);

    }
    public static double mainSau(double capacity,double n){
        if (capacity < 1) capacity = 1;
        double trafficSaturation = n / capacity;
        System.out.println("trafficSaturation=n/capacity="+trafficSaturation);
        trafficSaturation = Math.min(1.0, Math.max(0, trafficSaturation));
        return Math.round(trafficSaturation * 100.0) / 100.0;
    }
}