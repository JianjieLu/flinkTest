package whu.edu.moniData.UDPRece;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

public class UdpSenderDemo01 {

    public static void main(String[] args) throws IOException {
        // 创建UDP套接字（不绑定固定端口）
        DatagramSocket datagramSocket = new DatagramSocket();

        System.out.println("UDP发送器已启动，目标地址: 100.65.38.38:7171");
        System.out.println("输入消息并按回车发送 (输入 'bye' 退出)");

        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in));
        String data;
        byte[] datas;
        DatagramPacket datagramPacket;

        while (true) {
            // 准备数据 - 从控制台读取
            data = bufferedReader.readLine();
            datas = data.getBytes(StandardCharsets.UTF_8);

            // 发送到指定IP和端口
            datagramPacket = new DatagramPacket(
                    datas,
                    0,
                    datas.length,
                    new InetSocketAddress("127.0.0.1", 7171)
            );

            datagramSocket.send(datagramPacket);
            System.out.println("已发送: " + data);

            if(data.trim().equalsIgnoreCase("bye")) {
                System.out.println("正在关闭发送器...");
                datagramSocket.close();
                System.exit(0);
            }
        }
    }
}