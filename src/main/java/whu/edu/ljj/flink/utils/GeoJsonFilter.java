package whu.edu.ljj.flink.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class GeoJsonFilter {

    // 定义Jackson对象映射器
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        // 读取输入文件
        ObjectNode root = (ObjectNode) mapper.readTree(new File("D:\\learn\\codes\\a_idea_codes\\flinkTest\\src\\main\\java\\whu\\edu\\data\\chedao\\chedaoxian\\onlyBrokenLine.json"));

        // 获取features数组
        ArrayNode features = (ArrayNode) root.get("features");
        List<ObjectNode> filtered = new ArrayList<>();

        // 遍历所有要素
        for (int i = 0; i < features.size(); i++) {
            ObjectNode feature = (ObjectNode) features.get(i);
            ObjectNode geometry = (ObjectNode) feature.get("geometry");

            // 检查几何类型和坐标数量
            if (geometry != null &&
                    "LineString".equals(geometry.get("type").asText())) {
                ArrayNode coordinates = (ArrayNode) geometry.get("coordinates");
                if (coordinates.size() == 2) {
                    filtered.add(feature.deepCopy());
                }
            }
        }

        // 构建新数据
        ObjectNode newRoot = mapper.createObjectNode();
        newRoot.put("type", "FeatureCollection");
        newRoot.put("name", root.get("name").asText());

        ArrayNode newFeatures = newRoot.putArray("features");
        filtered.forEach(newFeatures::add);

        // 写入输出文件
        mapper.writerWithDefaultPrettyPrinter()
                .writeValue(new File("D:\\learn\\codes\\a_idea_codes\\flinkTest\\src\\main\\java\\whu\\edu\\data\\chedao\\chedaoxian\\output.json"), newRoot);

        System.out.println("处理完成，共保留 " + filtered.size() + " 个要素");
    }
}