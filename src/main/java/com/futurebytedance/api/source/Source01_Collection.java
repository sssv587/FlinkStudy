package com.futurebytedance.api.source;

import com.futurebytedance.api.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author yuhang.sun
 * @version 1.0
 * @date 2021/4/15 - 21:47
 * @Description FlinkAPI-source-从集合中读取数据
 */
public class Source01_Collection {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从集合中读取数据
        DataStreamSource<SensorReading> dataStream = env.fromCollection(Arrays.asList(
                new SensorReading("sensor_1", 1547718199L, 35.8),
                new SensorReading("sensor_6", 1547718201L, 15.4),
                new SensorReading("sensor_7", 1547718202L, 6.7),
                new SensorReading("sensor_10", 1547718205L, 38.1)
        ));

        DataStreamSource<Integer> integerDataStreamSource = env.fromElements(1, 2, 3, 4);

        //打印输出
        dataStream.print("data");
        integerDataStreamSource.print("int");

        //执行
        env.execute();

    }
}
