package com.flink.transform;

import com.entity.Sender;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Name Test5RichFunction
 * @Author shenYue
 * @Data 2022/1/24 10:38
 **/
public class Test5RichFunction {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<String> dataStream = env.readTextFile("D:\\SoftWare\\studyJava\\java\\work\\学习\\flink\\src\\main\\resources\\sender.txt");
        DataStream<Sender> map = dataStream.map(line -> {
            String[] split = line.split(",");
            return new Sender(split[0], Integer.valueOf(split[1]), split[2]);
        });

    }
}
