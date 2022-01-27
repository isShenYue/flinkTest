package com.flink.transform;

import com.entity.Sender;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.transformations.SinkTransformation;

import java.util.Collections;

/**
 * 分流  合流
 * @Name TestMinuteStream
 * @Author shenYue
 * @Data 2022/1/23 19:12
 **/
public class TestMinuteStream {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<String> dataStream = env.readTextFile("D:\\SoftWare\\studyJava\\java\\work\\学习\\flink\\src\\main\\resources\\sender.txt");
        DataStream<Sender> map = dataStream.map(line -> {
            String[] split = line.split(",");
            return new Sender(split[0], Integer.valueOf(split[1]), split[2]);
        });
        //分流
        SplitStream<Sender> splitStream = map.split(sender -> {
            return sender.getNumber() > 35 ? Collections.singletonList("big") : Collections.singletonList("dog");
        });
        DataStream<Sender> dog = splitStream.select("dog");
        SingleOutputStreamOperator<Tuple2<String, Integer>> streamOperator = dog.map(new MapFunction<Sender, Tuple2<String, Integer>>() {

            @Override
            public Tuple2<String, Integer> map(Sender sender) throws Exception {
                return Tuple2.of(sender.getName(), sender.getNumber());
            }
        });
        //合流
        ConnectedStreams<Sender, Tuple2<String, Integer>> connect = splitStream.select("big").connect(streamOperator);
        SingleOutputStreamOperator<Object> map1 = connect.map(new CoMapFunction<Sender, Tuple2<String, Integer>, Object>() {
            @Override
            public Object map1(Sender sender) throws Exception {
                return Tuple3.of(sender.getName(), sender.getNumber(), "old");
            }

            @Override
            public Object map2(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return Tuple2.of(stringIntegerTuple2.f0, "ooo");
            }
        });
        map1.print();
        env.execute();
    }
}
