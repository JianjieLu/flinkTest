package whu.edu.ljj;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;

import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

//统计文件中的一些单词的出现次数
public class WordCountBatchDemo {
    public static void main(String[] args) throws Exception {
        //1. 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //2.读取数据：从文件中读取
        DataSource<String> lineDS= env.readTextFile("input/word.txt");
        //3.切分、转换
        FlatMapOperator<String,Tuple2<String,Integer>> wordAndOne = lineDS.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {

            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                //按照空格切分单词
                String[] words = value.split(" ");
                //将单词转换为（word，1）
                for (String word : words) {
                    //使用collector向下游发送数据
                    out.collect(Tuple2.of(word,1));
                }
            }
        });
        //4.按照word分组
        UnsortedGrouping<Tuple2<String,Integer>>wordAndOneGroupby= wordAndOne.groupBy(0);
        //5.各分组内聚合
        AggregateOperator<Tuple2<String, Integer>> sum = wordAndOneGroupby.sum(1);//1是位置，表示tuple的第二个元素
        //6.输出
        sum.print();

    }
}
