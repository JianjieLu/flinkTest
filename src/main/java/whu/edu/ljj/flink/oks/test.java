package whu.edu.ljj.flink.oks;

import javafx.util.Pair;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class test {
    static Map<Long,Integer> IfLaterMap= new ConcurrentHashMap<>();
    static Map<Integer,Boolean> ifLaterMap= new ConcurrentHashMap<>();

    public static void main(String[] args) {
        IfLaterMap.put(123L,1);
        IfLaterMap.put(123L,IfLaterMap.get(123L)+1);
        System.out.println(IfLaterMap.get(123L));
        IfLaterMap.forEach((key, value) -> {
            System.out.println(key + "=" + value);
        });
        ifLaterMap.put(12,false);
        System.out.println(!ifLaterMap.get(12));
    }
}
