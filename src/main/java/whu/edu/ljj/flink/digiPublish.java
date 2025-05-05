package whu.edu.ljj.flink;
import org.zeromq.ZMQ;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
public class digiPublish {
    public static void main(String[] args) throws InterruptedException {
        ZMQ.Context context = ZMQ.context(1);
        ZMQ.Socket socket = context.socket(ZMQ.PUB);
        socket.bind("tcp://*:5563");
        List<String> list = readData("D:\\learn\\codes\\a_idea_codes\\flinkTest\\src\\main\\java\\whu\\edu\\ljj\\flink\\data\\mergedata\\yuanbancutted.txt");
//        List<String> list = readData("D:\\learn\\codes\\a_idea_codes\\flinkTest\\src\\main\\java\\whu\\edu\\data\\digiTest.txt");
        for (String str : list) {
            System.out.println(str);
             socket.send(str);
            Thread.sleep(300); // 每0.3秒发送一次
        }
    }
    private static List<String> readData(String path) {
            List<String> data= new ArrayList<>();
            try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue; // 跳过空行
                data.add(line);
            }
            return data;
        } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
}
}
