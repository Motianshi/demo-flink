package con.anqi.flink.state;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Map;

/**
 * 统计每种传感器每种水位值出现的次数
 */
public class KeyedReducingStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        DataStreamSource<Tuple2<String, Integer>> source1 = env.fromElements(
                Tuple2.of("a", 1), Tuple2.of("a", 11), Tuple2.of("a", 31), Tuple2.of("a", 53),
                Tuple2.of("b", 52), Tuple2.of("b", 81), Tuple2.of("b", 99), Tuple2.of("e", 111));


        SingleOutputStreamOperator<String> process = source1
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Tuple2<String, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((SerializableTimestampAssigner<Tuple2<String, Integer>>) (tuple, recordTime) -> tuple.f1 * 1000L)
                )
                .keyBy(k -> k.f0)
                .process(new KeyedProcessFunction<String, Tuple2<String, Integer>, String>() {

                    ReducingState<Integer> reducingState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ReducingStateDescriptor<Integer> reducingStateDescriptor = new ReducingStateDescriptor<>("mapState", new ReduceFunction<Integer>() {
                            @Override
                            public Integer reduce(Integer value1, Integer value2) throws Exception {
                                return value1 + value2;
                            }
                        }, Types.INT);
                        reducingState = getRuntimeContext().getReducingState(reducingStateDescriptor);
                        super.open(parameters);
                    }

                    @Override
                    public void processElement(Tuple2<String, Integer> value, KeyedProcessFunction<String, Tuple2<String, Integer>, String>.Context ctx, Collector<String> out) throws Exception {
                        // 来一条数据，添加到 reducing状态里
                        reducingState.add(value.f1);
                        Integer vcSum = reducingState.get();
                        out.collect("传感器id为" + value.f0 + ",水位值总和=" + vcSum);
//                                vcSumReducingState.get();   // 对本组的Reducing状态，获取结果
//                                vcSumReducingState.add();   // 对本组的Reducing状态，添加数据
//                                vcSumReducingState.clear(); // 对本组的Reducing状态，清空数据
                    }
                });
        process.print();

        env.execute();

    }
}
