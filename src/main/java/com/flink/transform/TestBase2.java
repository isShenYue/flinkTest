package com.flink.transform;

import com.entity.Sender;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Name TestBase2
 * @Author shenYue
 * @Data 2022/1/23 0:07
 **/
public class TestBase2 {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<String> dataStream = env.readTextFile("D:\\SoftWare\\studyJava\\java\\work\\学习\\flink\\src\\main\\resources\\sender.txt");
        DataStream<Sender> map = dataStream.map(line -> {
            String[] split = line.split(",");
            return new Sender(split[0], Integer.valueOf(split[1]), split[2]);
        });
        DataStream<Sender> name = map.keyBy(Sender::getName).maxBy("number");
        name.print();
        env.execute();
    }
}
