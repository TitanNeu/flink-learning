package indi.nyp;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @program: flink-java-learning
 * @description: word count
 * @author: Mr.Niu
 * @create: 2021-01-07 20:57
 **/

public class WordCount {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //从文件读取数据
        String path = "D:\\code\\java\\flink-java-learning\\src\\main\\resources\\hello.txt";
        DataSource<String> inputDataSet = env.readTextFile(path);
        //对数据集进行处理
        DataSet<Tuple2<String, Integer>> sum = inputDataSet.flatMap(new MyFlatmapFunction())
                .groupBy(0)
                .sum(1);
        sum.print();


    }
    public static class MyFlatmapFunction implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] strArray = s.split(" ");
            for (String str : strArray) {
                if (!str.isEmpty()) {
                    collector.collect(new Tuple2<>(str, 1));
                }
            }
        }
    }
}

