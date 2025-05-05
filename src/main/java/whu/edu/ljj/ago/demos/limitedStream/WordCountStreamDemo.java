package whu.edu.ljj.ago.demos.limitedStream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

//dataStream实现word计数
public class WordCountStreamDemo {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //读取数据:从文件读
        DataStreamSource<String> lineDS = env.readTextFile("input/word.txt");

        //处理数据:切分、转换、分组、聚合
        //切分、转换
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOneDS = lineDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = s.split(" ");
                for (String word : words) {
                    Tuple2<String, Integer> wordsAndOne = new Tuple2<>(word, 1);
                    collector.collect(wordsAndOne);
                }
            }
        });
        //分组
        KeyedStream<Tuple2<String, Integer>, String> wordAndOneks= wordAndOneDS.keyBy(
                new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                }
        );
        //聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumDS = wordAndOneks.sum(1);
        //输出数据
        sumDS.print();
        //执行

        env.execute();
    }
}
