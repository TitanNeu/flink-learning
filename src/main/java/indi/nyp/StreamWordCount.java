package indi.nyp;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @program: flink-java-learning
 * @description: 流式处理示例
 * @author: Mr.Niu
 * @create: 2021-01-13 21:35
 **/

public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String host = "localhost";
        int port = 10086;
        DataStreamSource<String> inputStreamSource = env.socketTextStream(host, port);
        DataStream<Tuple2<String, Integer>> outputStream = inputStreamSource.flatMap(new WordCount.MyFlatmapFunction())
                .keyBy(value->value.f0)
                .sum(1);
        outputStream.print();
        env.execute("stream-word-count");
    }



}

