package com.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @Name wordCount
 * @Author shenYue
 * @Data 2022/1/8 19:35
 **/
public class wordCount {
    /**
     * 批处理
     * @Author shenYue
     * @Date 2022/1/8 19:37
     * @Params [args]
     * @return void
     **/
    public static void main(String[] args) throws Exception {
        //批处理执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //读取文件内容
        DataSource<String> inputDataSet = env.readTextFile("D:\\SoftWare\\studyJava\\java\\work\\学习\\flink\\src\\main\\resources\\hello.txt");
        inputDataSet.print();
        //将数据转成map
        DataSet<Tuple2<String,Integer>> dataSet = inputDataSet.flatMap(new MyFlatMap()).groupBy(0).sum(1);
        dataSet.print();
    }

    public static class MyFlatMap implements FlatMapFunction<String, Tuple2<String,Integer>>{

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] strings = value.split(" ");
            for (String s: strings) {
                collector.collect(new Tuple2<>(s, 1));
            }
        }
    }
}
