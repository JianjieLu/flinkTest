package whu.edu.ljj.flink.utils;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.List;
import static whu.edu.ljj.flink.xiaohanying.Utils.*;

public class JsonReader {
    public static List<Location> readJsonFile(String filePath) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        File file = new File(filePath);
        return objectMapper.readValue(file, objectMapper.getTypeFactory().constructCollectionType(List.class, Location.class));
    }
    public static void getMax(String filePath) throws IOException {
            List<Location> l=readJsonFile(filePath);
            double minLon = l.get(0).getLongitude();
            double maxLon = minLon;
            double minLat = l.get(0).getLatitude();
            double maxLat = minLat;

            for (Location coord : l) {
                double lon = coord.getLongitude();
                double lat = coord.getLatitude();
                if (lon < minLon) minLon = lon;
                if (lon > maxLon) maxLon = lon;
                if (lat < minLat) minLat = lat;
                if (lat > maxLat) maxLat = lat;
            }

            System.out.println("经度最小值: " + minLon);
            System.out.println("经度最大值: " + maxLon);
            System.out.println("纬度最小值: " + minLat);
            System.out.println("纬度最大值: " + maxLat);
        }

    public static void main(String[] args) throws IOException {
        getMax("D:\\learn\\codes\\a_idea_codes\\flinkTest\\src\\main\\java\\whu\\edu\\ljj\\flink\\data\\zadaoGeojson\\DK_locations.json");
    }
}
