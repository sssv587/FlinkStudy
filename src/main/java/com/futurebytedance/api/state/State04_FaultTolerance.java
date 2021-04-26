package com.futurebytedance.api.state;

import com.futurebytedance.api.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author yuhang.sun
 * @version 1.0
 * @date 2021/4/26 - 0:27
 * @Description FlinkAPI-状态管理-状态后端
 */
public class State04_FaultTolerance {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.状态后端配置
        env.setStateBackend(new MemoryStateBackend());
        // env.setStateBackend(new FsStateBackend());
        // env.setStateBackend(new RocksDBStateBackend());

        // 2.检查点配置
        env.enableCheckpointing(300);

        // 高级选项
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(100);
        env.getCheckpointConfig().setPreferCheckpointForRecovery(true);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(10);

        // 3.重启策略配置
        // 固定延迟重启
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000L));
        // 失败率重启
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.minutes(10), Time.minutes(1)));

        //socket文本流
        DataStreamSource<String> inputStream = env.socketTextStream("localhost", 7777);

        //转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map((MapFunction<String, SensorReading>) value -> {
            String[] fields = value.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        });

        dataStream.print();

        env.execute();
    }
}
