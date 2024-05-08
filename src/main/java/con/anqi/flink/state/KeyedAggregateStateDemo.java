package con.anqi.flink.state;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
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

/**
 * 计算每种传感器的平均水位
 */
public class KeyedAggregateStateDemo {
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

                    AggregatingState<Integer, Double> aggState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        AggregatingStateDescriptor<Integer, Tuple2<Integer, Integer>, Double> aggDes = new AggregatingStateDescriptor<>("aggState", new AggregateFunction<Integer, Tuple2<Integer, Integer>, Double>() {
                            @Override
                            public Tuple2<Integer, Integer> createAccumulator() {
                                return Tuple2.of(0, 0);
                            }

                            @Override
                            public Tuple2<Integer, Integer> add(Integer val, Tuple2<Integer, Integer> tuple) {
                                return Tuple2.of(tuple.f0 + val, tuple.f1 + 1);
                            }

                            @Override
                            public Double getResult(Tuple2<Integer, Integer> tuple) {
                                return tuple.f0 * 1D / tuple.f1;
                            }

                            @Override
                            public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> integerIntegerTuple2, Tuple2<Integer, Integer> acc1) {
                                return null;
                            }
                        }, Types.TUPLE(Types.INT, Types.INT));
                        aggState = getRuntimeContext().getAggregatingState(aggDes);
                        super.open(parameters);
                    }

                    @Override
                    public void processElement(Tuple2<String, Integer> value, KeyedProcessFunction<String, Tuple2<String, Integer>, String>.Context ctx, Collector<String> out) throws Exception {
                        // 将 水位值 添加到  聚合状态中
                        aggState.add(value.f1);
                        // 从 聚合状态中 获取结果
                        Double vcAvg = aggState.get();

                        out.collect("传感器id为" + value.f0 + ",平均水位值=" + vcAvg);

//                                vcAvgAggregatingState.get();    // 对 本组的聚合状态 获取结果
//                                vcAvgAggregatingState.add();    // 对 本组的聚合状态 添加数据，会自动进行聚合
//                                vcAvgAggregatingState.clear();  // 对 本组的聚合状态 清空数据
                    }
                });
        process.print();

        env.execute();

    }
}
