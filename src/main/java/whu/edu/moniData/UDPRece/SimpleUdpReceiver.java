package whu.edu.moniData.UDPRece;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;

public class SimpleUdpReceiver {
    public static void main(String[] args) {
        try {
            // 绑定到指定IP和端口
            InetAddress address = InetAddress.getByName("100.65.38.38");
            DatagramSocket socket = new DatagramSocket(7171, address);
            byte[] buffer = new byte[4096];

            System.out.println("UDP Receiver started on " + address + ":7171");

            while (true) {
                // 接收数据包
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                socket.receive(packet);

                // 提取有效数据
                byte[] receivedData = new byte[packet.getLength()];
                System.arraycopy(buffer, 0, receivedData, 0, packet.getLength());

                // 打印基本信息
                System.out.println("\nReceived from " + packet.getAddress() + ":" + packet.getPort());
                System.out.println("Data length: " + receivedData.length + " bytes");

                // 打印十六进制原始数据
                System.out.println("Hex dump:");
                printHexDump(receivedData);

                // 打印ASCII原始数据（非打印字符显示为.）
                System.out.println("ASCII dump:");
                printAsciiDump(receivedData);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 十六进制打印工具
    private static void printHexDump(byte[] data) {
        for (int i = 0; i < data.length; i++) {
            System.out.printf("%02X ", data[i]);
            if ((i + 1) % 16 == 0) System.out.println();
        }
        System.out.println("\n");
    }

    // ASCII打印工具
    private static void printAsciiDump(byte[] data) {
        for (int i = 0; i < data.length; i++) {
            char c = (data[i] >= 32 && data[i] < 127) ? (char) data[i] : '.';
            System.out.print(c + "  ");
            if ((i + 1) % 16 == 0) System.out.println();
        }
        System.out.println("\n");
    }
}