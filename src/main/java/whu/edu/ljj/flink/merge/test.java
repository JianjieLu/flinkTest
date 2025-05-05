package whu.edu.ljj.flink.merge;

import whu.edu.ljj.flink.utils.myTools;

import static whu.edu.ljj.flink.xiaohanying.Utils.*;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class test {
    private static Map<Integer,String>s=new HashMap<>();
    public static void main(String[] args) {
    //        s.put(123,"132");
    //        String str=s.get(123);
    //        str="123";
    //        System.out.println(s.get(123));
            int i=2;
            int j=4;
            System.out.println((double)i/j );
    //        System.out.println(98000/60000*60000);

    //        try (BufferedWriter writer2 = new BufferedWriter(new FileWriter("D:\\learn\\codes\\a_idea_codes\\flinkTest\\src\\main\\java\\whu\\edu\\ljj\\flink\\data\\mergedata\\result\\result.txt",true))) {
    //
    //            writer2.write(System.lineSeparator() + "tempMap:");
    //            tempMap.forEach((key, value) -> {
    //                try {
    //                    writer2.write("key:" + key + "   " + "value:" + value);
    //                } catch (IOException e) {
    //                    throw new RuntimeException(e);
    //                }
    //            });
    //            writer2.write(System.lineSeparator() + "nowpmap:");
    //            nowMap.forEach((key, value) -> {
    //                try {
    //                    writer2.write("key:" + key + "   " + "value:" + value);
    //                } catch (IOException e) {
    //                    throw new RuntimeException(e);
    //                }
    //            });
    //            writer2.write(System.lineSeparator());
    //        }

            LinkedList<Float> s = new LinkedList<>();
            float a=1;
            float b=2;
            float v=3;
            s.add(a);
            s.add(b);
            s.add(v);
            LinkedList<Float> sp = s;
    //        deSpeedWindow(sp);
            ListIterator<Float> iterator = s.listIterator();
    //        while (iterator.hasNext()) {
    //            float originalValue = iterator.next();
    //            System.out.println(originalValue);
    //        }
            List<Integer> asc=new ArrayList<>();
    //        int j=0;
    //        for(int i = 0 ; i < 10 ; i ++){
    //            asc.add(i);
                j++;
    //        }
    String n="123";
    //        System.out.println(myTools.getNString(n,0,2));
            double v1 = Math.round(0 / (2) / ((double) (120000) / 60000) / ((double) 2200 / 60) * 100.0) / 100.0;
        System.out.println(v1);
    }
    private static void deSpeedWindow(LinkedList<Float> PathPointData){
        LinkedList<Float> speedWindow = PathPointData;
        ListIterator<Float> iterator = speedWindow.listIterator();
        while (iterator.hasNext()) {
            float originalValue = iterator.next();
            iterator.set(originalValue - 10);
            System.out.println(originalValue - 10);
        }
    }
}
