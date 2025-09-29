package whu.edu.moniData;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class test {

    public static void main(String[] args) {
        System.out.println((1757419620000L- 120000) / 10000 * 10000);
    }
    public static double mainSau(double capacity,double n){
        if (capacity < 1) capacity = 1;
        double trafficSaturation = n / capacity;
        System.out.println("trafficSaturation=n/capacity="+trafficSaturation);
        trafficSaturation = Math.min(1.0, Math.max(0, trafficSaturation));
        return Math.round(trafficSaturation * 100.0) / 100.0;
    }
}