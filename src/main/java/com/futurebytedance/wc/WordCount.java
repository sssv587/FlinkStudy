package com.futurebytedance.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


/**
 * @author yuhang.sun
 * @version 1.0
 * @date 2021/4/12 - 23:31
 * @Description 批处理WordCount
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 从文件中读取数据
        String inputPath = "C:\\Users\\10926\\IdeaProjects\\FlinkStudy\\src\\main\\resources\\hello.txt";
        DataSet<String> inputData = env.readTextFile(inputPath);

        // 对数据集进行处理，按空格分词展开，转换成(word,1)二元组进行统计
        AggregateOperator<Tuple2<String, Integer>> wordCountDataSet = inputData.flatMap(new MyFlatMapper())
                .groupBy(0) // 按照第一个位置的word分组
                .sum(1); // 将第二个位置上的数据求和

        // 打印输出
        wordCountDataSet.print();
    }

    // 自定义类，实现FlatMapFunction接口
    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // 按空格分词
            String[] words = value.split(" ");
            for (String word : words) {
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }
}
