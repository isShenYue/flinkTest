package com.flink;

import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Name StreamWordCount
 * @Author shenYue
 * @Data 2022/1/9 18:10
 **/
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        //流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //线程
        /*DataStreamSource<String> source = env.readTextFile("D:\\SoftWare\\studyJava\\java\\work\\学习\\flink\\src\\main\\resources\\streamhello.txt");
        source.print();*/
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        System.out.println(host);
        int port = parameterTool.getInt("port");
        System.out.println(port);
        DataStreamSource<String> source = env.socketTextStream(host, port);
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = source.flatMap(new wordCount.MyFlatMap()).keyBy(0).sum(1);
        sum.print();
        //开启执行任务
        env.execute();
    }
}
