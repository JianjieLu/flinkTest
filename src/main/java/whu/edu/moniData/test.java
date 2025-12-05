package whu.edu.moniData;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class test {
    public static int parseStake(String stakeId) {
        try {
            String[] parts = stakeId.split("\\+");
            return Integer.parseInt(parts[0]);
        } catch (Exception e) {
            return -1; // 无效桩号
        }
    }
    public static void main(String[] args) {
        String endStake=null;

        System.out.println(endStake.isEmpty());
    }
    public static List<Integer> mainSau(){
        List<Integer>l=new ArrayList<>();
        l.add(0);
        l.add(1);
        l.add(2);
        l.add(0);

        List<Integer> integers = calculateRanks(l);
        return integers;
    }
    public static List<Integer> calculateRanks(List<Integer> list) {
        // 创建一个索引列表，用于记录原始位置
        List<Integer> indices = new ArrayList<>();
        for (int i = 0; i < list.size(); i++) {
            indices.add(i);
        }

        // 根据数值降序排序索引（数值大的排名靠前）
        // 如果数值相同，保持原始顺序
        indices.sort((a, b) -> {
            int valueCompare = Integer.compare(list.get(b), list.get(a)); // 改为降序
            if (valueCompare == 0) {
                return Integer.compare(a, b); // 保持原始顺序
            }
            return valueCompare;
        });

        // 创建结果列表，初始值为0
        List<Integer> result = new ArrayList<>(Collections.nCopies(list.size(), 0));

        // 分配排名
        for (int rank = 0; rank < indices.size(); rank++) {
            int originalIndex = indices.get(rank);
            result.set(originalIndex, rank);
        }

        return result;
    }
}