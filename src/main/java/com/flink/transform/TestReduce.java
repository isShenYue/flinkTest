package com.flink.transform;

import com.entity.Sender;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * reduce
 * @Name TestReduce
 * @Author shenYue
 * @Data 2022/1/23 0:50
 **/
public class TestReduce {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<String> dataStream = env.readTextFile("D:\\SoftWare\\studyJava\\java\\work\\学习\\flink\\src\\main\\resources\\sender.txt");
        DataStream<Sender> map = dataStream.map(line -> {
            String[] split = line.split(",");
            return new Sender(split[0], Integer.valueOf(split[1]), split[2]);
        });
        KeyedStream<Sender, Tuple> keyedStream = map.keyBy("name");
        SingleOutputStreamOperator<Sender> reduce = keyedStream.reduce((oldSender, newSender) -> {
            return new Sender(oldSender.getName(), Math.max(oldSender.getNumber(), newSender.getNumber()), Math.max(Integer.parseInt(newSender.getTime()), Integer.parseInt(oldSender.getTime()))+"");
        });
        reduce.print();
        env.execute();
    }
}
