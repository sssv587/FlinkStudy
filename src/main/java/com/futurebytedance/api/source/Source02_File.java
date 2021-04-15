package com.futurebytedance.api.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author yuhang.sun
 * @version 1.0
 * @date 2021/4/15 - 22:01
 * @Description FlinkAPI-source-从文件中读取数据
 */
public class Source02_File {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从文件读取数据
        DataStreamSource<String> dataStream = env.readTextFile("C:\\Users\\10926\\IdeaProjects\\FlinkStudy\\doc\\txt\\sensor.txt");

        //打印输出
        dataStream.print();

        env.execute();
    }
}
