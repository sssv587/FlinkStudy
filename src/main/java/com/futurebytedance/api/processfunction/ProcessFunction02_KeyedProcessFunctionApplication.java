package com.futurebytedance.api.processfunction;

import com.futurebytedance.api.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author yuhang.sun
 * @version 1.0
 * @date 2021/4/26 - 22:39
 * @Description FlinkAPI - KeyedProcessFunctionApplication
 */
public class ProcessFunction02_KeyedProcessFunctionApplication {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //socket文本流
        DataStreamSource<String> inputStream = env.socketTextStream("localhost", 7777);

        //转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map((MapFunction<String, SensorReading>) value -> {
            String[] fields = value.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        });


        dataStream.keyBy("id")
                .process(new TempConIncreaseWarning(10))
                .print();

        env.execute();
    }

    // 实现自定义处理函数，检测一段时间内的温度连续上升，输出报警
    public static class TempConIncreaseWarning extends KeyedProcessFunction<Tuple, SensorReading, String> {
        // 定义私有属性，当前统计的时间间隔区域
        private final Integer interval;

        public TempConIncreaseWarning(Integer interval) {
            this.interval = interval;
        }

        // 定义状态，保存上一次的温度值，定时器时间戳
        private ValueState<Double> lastTempState;
        private ValueState<Long> timerTsState;

        @Override
        public void open(Configuration parameters) {
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<>("last-temp", Double.class, Double.MIN_VALUE));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<>("timer-ts", Long.class));
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
            // 取出状态
            Double lastTemp = lastTempState.value();
            Long timeTs = timerTsState.value();

            // 如果温度上升并且没有定时器，注册十秒后的定时器，开始等待
            if (value.getTemperature() > lastTemp && timeTs == null) {
                // 计算出定时器时间戳
                long ts = ctx.timerService().currentProcessingTime() + interval * 1000;
                ctx.timerService().registerProcessingTimeTimer(ts);
                timerTsState.update(ts);
            }
            // 如果温度下降，那么删除定时器
            else if (value.getTemperature() < lastTemp && timeTs != null) {
                ctx.timerService().deleteProcessingTimeTimer(timeTs);
                timerTsState.clear();
            }

            // 更新温度状态
            lastTempState.update(value.getTemperature());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) {
            // 定时器触发，输出报警信息
            out.collect("传感器" + ctx.getCurrentKey().getField(0) + "温度值连续" + interval + "上升");
            timerTsState.clear();
        }

        @Override
        public void close() {
            lastTempState.clear();
        }
    }
}
