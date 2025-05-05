package whu.edu.ljj.flink.merge.Version1;

import com.alibaba.fastjson2.JSON;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.util.Collector;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;


import static whu.edu.ljj.flink.xiaohanying.Utils.*;

public class AnomalyDetectv3 {
    public static class CustomBucketAssigner implements BucketAssigner<GantryRecord, String>, Serializable{
        private static final long serialVersionUID = 1L;

        private static final DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");;
        private static final DateTimeFormatter outputFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");

        @Override
        public String getBucketId(GantryRecord gantryRecord, Context context) {
            // 提取时间戳
            String timestamp = gantryRecord.getUploadTime();
            LocalDateTime dateTime = LocalDateTime.parse(timestamp, inputFormatter);
            return dateTime.format(outputFormatter) + "_GantryRecords";
        }

        @Override
        public SimpleVersionedStringSerializer getSerializer() {
            return SimpleVersionedStringSerializer.INSTANCE;
        }
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    public static class GantryRecord implements Serializable {
        private String uploadTime;
        private int allGantrySum = 0;
        private int anomalyGantrySum = 0;
        private List<GantryData> misGantries = new ArrayList<>();

        @Override
        public String toString() {
            return JSON.toJSONString(this);
        }
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    public static class GantryImage implements Serializable {
        // 存储的时候为prefix(direction(device) (+sp)) + uploadTime + plateNumber
        // gantryKey为uploadTime + plateNumber构成
        private String plateNumber = null;
        private String headImage;
        private String uploadTime;
        private String prefix = "";
        private long mileage;
    }

    public static class ImageFileSink extends RichSinkFunction<GantryImage> implements Serializable {

        private static final DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");;
        private static final DateTimeFormatter outputFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");

        private final String basePath;        // 根目录（如 "file:///data/images"）
        private final String fileExtension = ".jpg";   // 文件后缀（如 ".jpg"）

        public ImageFileSink(String basePath) {
            this.basePath = basePath;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            // 初始化根目录（确保存在）
            new File(basePath).mkdirs();
        }

        @Override
        public void invoke(GantryImage event, Context context) throws Exception {
            // 1. 解析时间戳，生成日期目录（如 "20250326_AnomalyPics"）
            String timestamp = event.getUploadTime();
            LocalDateTime dateTime = LocalDateTime.parse(timestamp, inputFormatter);
            String dirName = dateTime.format(outputFormatter).replace(":", "_") + "_AnomalyPics";

            // 2. 构建完整输出路径（如 /data/images/20250326_AnomalyPics/）
            String outputDir = basePath + File.separator + dirName;
            Files.createDirectories(Paths.get(outputDir)); // 自动创建目录

            // 3. 生成文件名（如 "2714AC28-984A-42E9-A001-36395F243E99.jpg"）
            String fileName = event.getPrefix() + event.getUploadTime().replace(":", "_") + event.getPlateNumber() +fileExtension;
            String filePath = outputDir + File.separator + fileName;

            // 4. 解码 Base64 并写入文件
            // 特殊情况：没有HeadImage？？？-> envstate = 99时没有headImage
            if(event.getHeadImage() != null) {
                byte[] imageBytes = Base64.getDecoder().decode(event.getHeadImage());
                try (FileOutputStream fos = new FileOutputStream(filePath)) {
                    fos.write(imageBytes);
                }
            }
        }
    }

    // 下载所有无牌车和不匹配的情况
    public static class AnomalyGantryToPic extends CoProcessFunction<GantryRecord, GantryImage, GantryImage> implements Serializable{

        private MapState<String, GantryImage> gantryImageState;
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.seconds(20))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();

        @Override
        public void open(Configuration parameters) {
            // 定义门架数据缓存状态（Key 是门架时间戳）
            MapStateDescriptor<String, GantryImage> gantryImageDescriptor =
                    new MapStateDescriptor<>("gantryImageState", Types.STRING, TypeInformation.of(new TypeHint<GantryImage>() {
                    }));
            gantryImageDescriptor.enableTimeToLive(ttlConfig);
            gantryImageState = getRuntimeContext().getMapState(gantryImageDescriptor);
        }

        @Override
        public void processElement1(GantryRecord gantryRecord, CoProcessFunction<GantryRecord, GantryImage, GantryImage>.Context ctx, Collector<GantryImage> out) throws Exception {
            if(!gantryRecord.getMisGantries().isEmpty())
            {
                for(GantryData gantry : gantryRecord.getMisGantries())
                {
                    String gantryKey;
                    if(gantry.getTollPlateNumber() != null)
                        gantryKey = gantry.getUploadTime() + gantry.getTollPlateNumber();
                    else if(gantry.getPlateNumber().equals("默A00000"))
                        continue;
                    else
                        gantryKey = gantry.getUploadTime() + gantry.getPlateNumber();
                    System.out.println("\n即将要参与记录的gantryKey：" + gantryKey.replace(":", "-"));

                    // 这里为什么可能是null？gantryImageState分明是全都有的
                    if(!gantryImageState.contains(gantryKey))
                    {
                        String outputFile = "D:\\temp\\notStateGantryImage.txt";  // 输出文件名

                        System.out.println("出现异常，有tollList但是无tollPlateNumber，记录gantry：" + gantryKey);
                        try(FileWriter fileWriter = new FileWriter(outputFile, true);
                            PrintWriter printWriter = new PrintWriter(fileWriter)) {
                            printWriter.println(com.alibaba.fastjson2.JSON.toJSONString(gantry));
                        }
                    }
                    else
                        out.collect(gantryImageState.get(gantryKey));
                }
            }
        }

        @Override
        public void processElement2(GantryImage gantryImage, CoProcessFunction<GantryRecord, GantryImage, GantryImage>.Context ctx, Collector<GantryImage> out) throws Exception {
            gantryImageState.put(gantryImage.getUploadTime() + gantryImage.getPlateNumber(), gantryImage);
        }
    }

    public static boolean convertBase64ToImage(String base64Data, String filePath) {
        try {
            // 解码 Base64 数据
            byte[] imageBytes = Base64.getDecoder().decode(base64Data);

            // 创建文件输出流
            File outputFile = new File(filePath);
            try (FileOutputStream fileOutputStream = new FileOutputStream(outputFile)) {
                // 将字节数据写入文件
                fileOutputStream.write(imageBytes);
            }

            return true; // 成功保存
        } catch (IOException e) {
            e.printStackTrace();
            return false; // 保存失败
        }
    }
}
