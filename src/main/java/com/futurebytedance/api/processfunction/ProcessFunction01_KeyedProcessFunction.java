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
 * @date 2021/4/26 - 0:52
 * @Description FlinkAPI-ProcessFunction
 */
public class ProcessFunction01_KeyedProcessFunction {
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

        // 测试KeyedProcessFunction，先分组，然后自定义处理
        dataStream.keyBy("id")
                .process(new MyProcess())
                .print();

        env.execute();
    }

    // 实现自定义的处理函数
    public static class MyProcess extends KeyedProcessFunction<Tuple, SensorReading, Integer> {
        private ValueState<Long> tsTimerState;

        @Override
        public void open(Configuration parameters) {
            tsTimerState = getRuntimeContext().getState(new ValueStateDescriptor<>("ts-timer", Long.class));
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<Integer> out) throws Exception {
            out.collect(value.getId().length());

            // context
            ctx.timestamp();
            ctx.getCurrentKey();
//            ctx.output();

            ctx.timerService().currentProcessingTime();
            ctx.timerService().currentWatermark();

            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 1000L);
            tsTimerState.update(ctx.timerService().currentProcessingTime() + 1000L);
//            ctx.timerService().registerEventTimeTimer((value.getTimestamp() + 10) * 1000);

//            ctx.timerService().deleteEventTimeTimer(10000L);
            ctx.timerService().deleteProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 1000L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Integer> out) {
            System.out.println(timestamp + " 定时器触发");
            ctx.getCurrentKey();
//            ctx.output();
            ctx.timeDomain();
        }

        @Override
        public void close() {
            tsTimerState.clear();
        }
    }
}
