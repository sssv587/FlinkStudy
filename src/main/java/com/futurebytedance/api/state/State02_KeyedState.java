package com.futurebytedance.api.state;

import com.futurebytedance.api.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author yuhang.sun
 * @version 1.0
 * @date 2021/4/25 - 22:28
 * @Description FlinkAPI-Keyed_State
 */
public class State02_KeyedState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //socket文本流
        DataStreamSource<String> inputStream = env.socketTextStream("localhost", 7777);

        //转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map((MapFunction<String, SensorReading>) value -> {
            String[] fields = value.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        });

        SingleOutputStreamOperator<Integer> resultStream = dataStream
                .keyBy("id")
                .map(new MyKeyCountMapper());

        resultStream.print();

        env.execute();
    }

    //自定义RichMapFunction
    public static class MyKeyCountMapper extends RichMapFunction<SensorReading, Integer> {
        private ValueState<Integer> keyCountState;

        //其他类型状态的声明
        // private ListState<String> myListState;
        // private MapState<String, Double> myMapState;
        // private ReducingState<SensorReading> myReducingState;

        @Override
        public void open(Configuration parameters) {
            keyCountState = getRuntimeContext()
                    .getState(new ValueStateDescriptor<>("key-count", Integer.class, 0));
//            myListState = getRuntimeContext()
//                    .getListState(new ListStateDescriptor<>("my-list", String.class));
//            myMapState = getRuntimeContext()
//                    .getMapState(new MapStateDescriptor<String, Double>("my-map", String.class, Double.class));
//            myReducingState = getRuntimeContext()
//                    .getReducingState(new ReducingStateDescriptor<SensorReading>("my-reducing", new ReduceFunction<SensorReading>() {
//                        @Override
//                        public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {
//                            return new SensorReading(value1.getId(), value1.getTimestamp(), value1.getTemperature() + value2.getTemperature());
//                        }
//                    }, SensorReading.class));
        }

        @Override
        public Integer map(SensorReading value) throws Exception {
            // 其他状态API调用
            // list state
            // for (String str : myListState.get()){
            //     System.out.println(str);
            // }
            // myListState.add("hello");

            // map state
            // myMapState.get("1");
            // myMapState.put("2", 12.3);
            // myMapState.remove("2");

            // reducing state
            // myReducingState.add(value);
            // myReducingState.clear();


            Integer count = keyCountState.value();
            count++;
            keyCountState.update(count);
            return count;

        }
    }
}
