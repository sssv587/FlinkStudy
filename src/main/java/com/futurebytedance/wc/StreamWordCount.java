package com.futurebytedance.wc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author yuhang.sun
 * @version 1.0
 * @date 2021/4/13 - 0:52
 * @Description 流计算处理WordCount
 */
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        // 创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(8);
        //不合并前后算子  one-to-one/分区数一样
        //env.disableOperatorChaining();

        // 从文件中读取
//        String inputPath = "C:\\Users\\10926\\IdeaProjects\\FlinkStudy\\src\\main\\resources\\hello.txt";
//        DataStreamSource<String> inputDataStream = env.readTextFile(inputPath);

        // 用parameter tool 工具从程序启动参数中提取配置项
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");

        // 从socket文件流读取数据
        DataStreamSource<String> inputDataStream = env.socketTextStream(host, port);

        // 基于数据流进行转换计算
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultStream = inputDataStream
                .flatMap(new WordCount.MyFlatMapper()).slotSharingGroup("grep")
                .keyBy(0)
                .sum(1)
                .setParallelism(2).slotSharingGroup("red");
        //开始新的任务链
        //.startNewChain();
        //.disableChaining();

        resultStream.print();

        //执行任务
        env.execute();
    }
}
