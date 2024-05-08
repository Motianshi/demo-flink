package con.anqi.flink.state;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class StateTTLDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

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

                    ValueState<Integer> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        // TODO 1.创建 StateTtlConfig
                        StateTtlConfig stateTtlConfig = StateTtlConfig
                                .newBuilder(Time.seconds(5)) // 过期时间5s
//                                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite) // 状态 创建和写入（更新） 更新 过期时间
                                .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite) // 状态 读取、创建和写入（更新） 更新 过期时间
                                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired) // 不返回过期的状态值
                                .build();

                        // TODO 2.状态描述器 启用 TTL
                        ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<>("lastVcState", Types.INT);
                        stateDescriptor.enableTimeToLive(stateTtlConfig);

                        this.valueState = getRuntimeContext().getState(stateDescriptor);
                    }

                    @Override
                    public void processElement(Tuple2<String, Integer> value, KeyedProcessFunction<String, Tuple2<String, Integer>, String>.Context ctx, Collector<String> out) throws Exception {
                        // 先获取状态值，打印 ==》 读取状态
                        Integer lastVc = valueState.value();
                        out.collect("key=" + value.f1 + ",状态值=" + lastVc);

                        // 如果水位大于10，更新状态值 ===》 写入状态
                        if (value.f1 > 10) {
                            valueState.update(value.f1);
                        }
                    }
                });
        process.print();

        env.execute();

    }
}
