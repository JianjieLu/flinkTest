package whu.edu.moniData.shenZhou.ke3Buquan;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import whu.edu.moniData.shenZhou.ke3Buquan.DataUtils.*;

public class KDTree {

    private static final double EARTH_RADIUS = 6371.393;

    private Node kdtree;

    private class Node{
        //分割的维度
        int partitionDimention;
        //分割的值
        double partitionValue;
        //如果为非叶子节点，该属性为空
        //否则为数据
        SpatialPoint value;
        //是否为叶子
        boolean isLeaf=false;
        //左树
        Node left;
        //右树
        Node right;
        //每个维度的最小值
        double[] min;
        //每个维度的最大值
        double[] max;
    }

    private static class UtilZ{
        /**
         * 计算给定维度的方差
         * @param data 数据
         * @param dimention 维度
         * @return 方差
         */
        static double variance(List<SpatialPoint> data, int dimention){
            double vsum = 0;
            double sum = 0;
            for(SpatialPoint sp : data){
                double[] d = sp.getCoordinate();
                sum += d[dimention];
                vsum += d[dimention]*d[dimention];
            }
            int n = data.size();
            return vsum/n-Math.pow(sum/n, 2);
        }
        /**
         * 取排序后的中间位置数值
         * @param data 数据
         * @param dimention 维度
         * @return
         */
        static double median(List<SpatialPoint> data,int dimention){
            double[] d = new double[data.size()];
            int i = 0;
            for(SpatialPoint sp : data){
                double[] k = sp.getCoordinate();
                d[i++] = k[dimention];
            }
            return findPos(d, 0, d.length-1, d.length/2);
        }

        static double[][] maxmin(List<SpatialPoint> data, int dimentions){
            double[][] mm = new double[2][dimentions];
            //初始化 第一行为min，第二行为max
            for(int i = 0; i < dimentions; i++) {
                mm[0][i] = mm[1][i] = data.get(0).getCoordinate()[i];
                for(int j = 1; j<data.size(); j++) {
                    double[] d = data.get(j).getCoordinate();
                    if(d[i] < mm[0][i]){
                        mm[0][i] = d[i];
                    }else if(d[i] > mm[1][i]) {
                        mm[1][i] = d[i];
                    }
                }
            }
            return mm;
        }

        static double distance(double[] a,double[] b){
            double lon1 = a[0];
            double lat1 = a[1];

            double lon2 = b[0];
            double lat2 = b[1];

            // 将角度转换为弧度
            double dLat = Math.toRadians(lat2 - lat1);
            double dLon = Math.toRadians(lon2 - lon1);
            lat1 = Math.toRadians(lat1);
            lat2 = Math.toRadians(lat2);

            // Haversine 公式
            double aa = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
                    Math.cos(lat1) * Math.cos(lat2) *
                            Math.sin(dLon / 2) * Math.sin(dLon / 2);
            double cc = 2 * Math.atan2(Math.sqrt(aa), Math.sqrt(1 - aa));
            double distance = EARTH_RADIUS * cc;

            return distance * 1000;
        }

        /**
         * 在max和min表示的超矩形中的点和点a的最小距离
         * @param a 点a
         * @param max 超矩形各个维度的最大值
         * @param min 超矩形各个维度的最小值
         * @return 超矩形中的点和点a的最小距离
         */
        static double mindistance(double[] a, double[] max, double[] min){
            double lon = a[0]; // 目标点经度
            double lat = a[1]; // 目标点纬度

            // 检查点是否在矩形内（经纬度范围）
            boolean withinLon = (lon >= min[0] && lon <= max[0]);
            boolean withinLat = (lat >= min[1] && lat <= max[1]);

            // 情况1：点在矩形内 -> 距离为0（简化处理，实际应为到边界的最小距离）
            if (withinLon && withinLat) {
                return 0;
            }

            // 情况2：点在矩形外 -> 计算到矩形边界的最小球面距离
            List<double[]> boundaryPoints = new ArrayList<>();

            // 1. 计算四个顶点
            boundaryPoints.add(new double[]{min[0], min[1]}); // 左下角
            boundaryPoints.add(new double[]{min[0], max[1]}); // 左上角
            boundaryPoints.add(new double[]{max[0], min[1]}); // 右下角
            boundaryPoints.add(new double[]{max[0], max[1]}); // 右上角

            // 2. 计算经度边界上的最近点（如果点在经度范围内，但不在纬度范围内）
            if (withinLon) {
                if (lat < min[1]) {
                    boundaryPoints.add(new double[]{lon, min[1]}); // 下边界最近点
                } else {
                    boundaryPoints.add(new double[]{lon, max[1]}); // 上边界最近点
                }
            }

            // 3. 计算纬度边界上的最近点（如果点在纬度范围内，但不在经度范围内）
            if (withinLat) {
                if (lon < min[0]) {
                    boundaryPoints.add(new double[]{min[0], lat}); // 左边界最近点
                } else {
                    boundaryPoints.add(new double[]{max[0], lat}); // 右边界最近点
                }
            }

            // 4. 计算所有边界点中最近的距离
            double minDistance = Double.MAX_VALUE;
            for (double[] point : boundaryPoints) {
                double dist = distance(new double[]{lon, lat}, new double[]{point[0], point[1]});
                if (dist < minDistance) {
                    minDistance = dist;
                }
            }

            return minDistance;
        }

        /**
         * 使用快速排序，查找排序后位置在point处的值
         * 比Array.sort()后去对应位置值，大约快30%
         * @param data 数据
         * @param low 参加排序的最低点
         * @param high 参加排序的最高点
         * @param point 位置
         * @return
         */
        private static double findPos(double[] data,int low,int high,int point){
            int lowt=low;
            int hight=high;
            double v = data[low];
            ArrayList<Integer> same = new ArrayList<Integer>((int)((high-low)*0.25));
            while(low<high){
                while(low<high&&data[high]>=v){
                    if(data[high]==v){
                        same.add(high);
                    }
                    high--;
                }
                data[low]=data[high];
                while(low<high&&data[low]<v)
                    low++;
                data[high]=data[low];
            }
            data[low]=v;
            int upper = low+same.size();
            if (low<=point&&upper>=point) {
                return v;
            }

            if(low>point){
                return findPos(data, lowt, low-1, point);
            }

            int i=low+1;
            for(int j:same){
                if(j<=low+same.size())
                    continue;
                while(data[i]==v)
                    i++;
                data[j]=data[i];
                data[i]=v;
                i++;
            }

            return findPos(data, low+same.size()+1, hight, point);
        }
    }

    private KDTree() {}

    // 添加构建任务类
    private static class BuildTask {
        Node node;
        List<SpatialPoint> data;
        int dimensions;

        BuildTask(Node node, List<SpatialPoint> data, int dimensions) {
            this.node = node;
            this.data = data;
            this.dimensions = dimensions;
        }
    }

    // 修改构建方法
    private void buildDetail(Node root, List<SpatialPoint> data, int dimensions) {
        Stack<BuildTask> stack = new Stack<>();
        stack.push(new BuildTask(root, data, dimensions));

        while (!stack.isEmpty()) {
            BuildTask task = stack.pop();
            Node node = task.node;
            List<SpatialPoint> currentData = task.data;
            int dims = task.dimensions;

            if (currentData.isEmpty()) {
                continue;
            }

            if (currentData.size() == 1) {
                initLeaf(node, currentData.get(0));
                continue;
            }

            // 选择分割维度
            node.partitionDimention = -1;
            double maxVariance = -1;
            for (int i = 0; i < dims; i++) {
                double variance = UtilZ.variance(currentData, i);
                if (variance > maxVariance) {
                    maxVariance = variance;
                    node.partitionDimention = i;
                }
            }

            // 处理全相同点的情况
            if (maxVariance == 0) {
                initLeaf(node, currentData.get(0));
                continue;
            }

            // 获取分割值
            node.partitionValue = UtilZ.median(currentData, node.partitionDimention);

            // 设置边界
            double[][] bounds = UtilZ.maxmin(currentData, dims);
            node.min = bounds[0];
            node.max = bounds[1];

            // 分割数据
            List<SpatialPoint> left = new ArrayList<>();
            List<SpatialPoint> right = new ArrayList<>();

            for (SpatialPoint point : currentData) {
                double[] coord = point.getCoordinate();
                if (coord[node.partitionDimention] < node.partitionValue) {
                    left.add(point);
                } else {
                    right.add(point);
                }
            }

            // 优化点1：防止无效分割
            if (left.isEmpty() || right.isEmpty()) {
                initLeaf(node, currentData.get(0));
                continue;
            }

            // 创建子节点
            node.left = new Node();
            node.right = new Node();

            // 优化点2：先处理较大的子树以减少栈深度
            if (left.size() > right.size()) {
                stack.push(new BuildTask(node.right, right, dims));
                stack.push(new BuildTask(node.left, left, dims));
            } else {
                stack.push(new BuildTask(node.left, left, dims));
                stack.push(new BuildTask(node.right, right, dims));
            }
        }
    }

    private void initLeaf(Node node, SpatialPoint point) {
        node.isLeaf = true;
        node.value = point;
    }

    /**
     * 构建树
     * @param input 输入
     * @return KDTree树
     */
    // 修改构建入口点
    public static KDTree build(List<SpatialPoint> input) {
        if (input == null || input.isEmpty()) {
            return new KDTree(); // 返回空树
        }

        int m = input.get(0).getCoordinate().length;
        KDTree tree = new KDTree();
        tree.kdtree = tree.new Node();
        tree.buildDetail(tree.kdtree, input, m);
        return tree;
    }


    /**
     * 打印树，测试时用
     */
    public void print(){
        printRec(kdtree,0);
    }

    private void printRec(Node node,int lv){
        if(!node.isLeaf){
            for(int i=0;i<lv;i++)
                System.out.print("--");
            System.out.println(node.partitionDimention+":"+node.partitionValue);
            printRec(node.left,lv+1);
            printRec(node.right,lv+1);
        }else {
            for(int i=0;i<lv;i++)
                System.out.print("--");
            StringBuilder s = new StringBuilder();
            s.append('(');
            for(int i=0;i<node.value.getCoordinate().length-1;i++){
                s.append(node.value.getCoordinate()[i]).append(',');
            }
            s.append(node.value.getCoordinate()[node.value.getCoordinate().length-1]).append(')');
            System.out.println(s);
        }
    }

    public SpatialPoint query(double[] input){
        Node node = kdtree;
        Stack<Node> stack = new Stack<Node>();
        while(!node.isLeaf){
            if(input[node.partitionDimention]<node.partitionValue){
                stack.add(node.right);
                node=node.left;
            }else{
                stack.push(node.left);
                node=node.right;
            }
        }
        /**
         * 首先按树一路下来，得到一个想对较近的距离，再找比这个距离更近的点
         */
        double distance = UtilZ.distance(input, node.value.getCoordinate());
        SpatialPoint nearest=queryRec(input, distance, stack);
        return nearest==null? node.value:nearest;
    }

    public SpatialPoint queryRec(double[] input, double distance, Stack<Node> stack){
        SpatialPoint nearest = null;
        Node node = null;
        double tdis;
        while(stack.size()!=0){
            node = stack.pop();
            if(node.isLeaf){
                tdis=UtilZ.distance(input, node.value.getCoordinate());
                if(tdis<distance){
                    distance = tdis;
                    nearest = node.value;
                }
            }else {
                /*
                 * 得到该节点代表的超矩形中点到查找点的最小距离mindistance
                 * 如果mindistance<distance表示有可能在这个节点的子节点上找到更近的点
                 * 否则不可能找到
                 */
                double mindistance = UtilZ.mindistance(input, node.max, node.min);
                if (mindistance<distance) {
                    while(!node.isLeaf){
                        if(input[node.partitionDimention]<node.partitionValue){
                            stack.add(node.right);
                            node=node.left;
                        }else{
                            stack.push(node.left);
                            node=node.right;
                        }
                    }
                    tdis=UtilZ.distance(input, node.value.getCoordinate());
                    if(tdis<distance){
                        distance = tdis;
                        nearest = node.value;
                    }
                }
            }
        }
        return nearest;
    }

    /**
     * 线性查找，用于和kdtree查询做对照
     * 1.判断kdtree实现是否正确
     * 2.比较性能
     * @param input
     * @param data
     * @return
     */
    public static double[] nearest(double[] input,double[][] data){
        double[] nearest=null;
        double dis = Double.MAX_VALUE;
        double tdis;
        for(int i=0;i<data.length;i++){
            tdis = UtilZ.distance(input, data[i]);
            if(tdis<dis){
                dis=tdis;
                nearest = data[i];
            }
        }
        return nearest;
    }
}
