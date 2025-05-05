package whu.edu.ljj.ago.demos;

import java.util.*;

public class demo {
    public static void main(String[] args) {
        int a=10;
        int b=3;
        long c=10;
        String d="abc";
        long start;
        long timeobs=1200;
        long timesplit=30;
        long starttime=1000;
        long index=(timeobs-starttime)/timesplit;
        start=starttime+timesplit*index;
        String numStr = String.valueOf(23523532511251L);
        ;
         Map<String, List<String>> map = new LinkedHashMap<>();
         Map<String, List<String>> map1 = new LinkedHashMap<>();
        List<String> list1 = new ArrayList<>(Arrays.asList("apple", "banana", "orange"));
        List<String> list2 = new ArrayList<>(Arrays.asList("dog", "cat", "bird"));
map1.put("apple", list1);
        // 将String和List<String>的映射关系存入Map
        map.put("fruit", list1);
        map.put("animal", list2);
            List<String> list1FromMap = map.get("fruit");
            list1FromMap.add("lujj");

            if(map.get("xyzz")==null){
                List<String> list3 = new ArrayList<>(Arrays.asList("1", "2", "3"));
                map.put("xyzz", list3);
            }
            map1=map;
            test();
        System.out.println(map1.get("xyzz"));
    }
     public static String getLastNString(String input,int num) {
        return input.substring(input.length() - num);
    }
    public static String getLastNString(String input) {
         // 创建一个LinkedHashMap，键为String，值为List<String>
        Map<String, List<String>> map = new LinkedHashMap<>();
        List<String> list1 = new ArrayList<>(Arrays.asList("apple", "banana", "orange"));
        List<String> list2 = new ArrayList<>(Arrays.asList("dog", "cat", "bird"));

        // 将String和List<String>的映射关系存入Map
        map.put("fruit", list1);
        map.put("animal", list2);
            List<String> list1FromMap = map.get("fruit");
            list1FromMap.add("lujj");
        // 通过String键来索引List<String>
        String key = "fruit";
        if (map.containsKey(key)) {
            List<String> indexedList = map.get(key);
            System.out.println("索引到的List：" + indexedList);
        } else {
            System.out.println("未找到对应的List");
        }
        return input;
    }
    public static void test(){
        Map<String, Integer> mapJudge = new HashMap<>();
Map<String, Integer> temp = new HashMap<>();

// 示例数据
mapJudge.put("a", 1);
mapJudge.put("b", 2);
mapJudge.put("c", 3);
temp.put("a", 1);
temp.put("b", 2);
temp.put("d", 3);
Set<String> keysToRemove = new HashSet<>(temp.keySet());
mapJudge.keySet().removeAll(keysToRemove);
 Set<String> keys = mapJudge.keySet();
  for (String key : keys) {
            System.out.println("Key: " + key);
        }
// 输出结果
System.out.println("Unique entries in mapJudge: " + mapJudge);

//// 使用流API找出mapJudge中有而temp中没有的键值对
//Map<String, Integer> uniqueEntries = mapJudge.entrySet().stream()
//    .filter(entry -> !temp.containsKey(entry.getKey()))
//    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
//
//// 输出结果
//System.out.println("Unique entries in mapJudge: " + uniqueEntries);
//Set<String> keys = uniqueEntries.keySet();
//for (String key : keys) {
//    System.out.println("Key: " + key);
//}
    }
}
