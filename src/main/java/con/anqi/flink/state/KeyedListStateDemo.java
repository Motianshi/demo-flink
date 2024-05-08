package con.anqi.flink.state;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * 针对每种传感器输出最高的3个水位值
 */
public class KeyedListStateDemo {
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

                    ListState<Integer> listState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        listState = getRuntimeContext().getListState(new ListStateDescriptor<>("valueState", Types.INT));
                        super.open(parameters);
                    }

                    @Override
                    public void processElement(Tuple2<String, Integer> value, KeyedProcessFunction<String, Tuple2<String, Integer>, String>.Context ctx, Collector<String> out) throws Exception {
                        // 1.来一条，存到list状态里
                        listState.add(value.f1);

                        // 2.从list状态拿出来(Iterable)， 拷贝到一个List中，排序， 只留3个最大的
                        Iterable<Integer> vcListIt = listState.get();
                        // 2.1 拷贝到List中
                        List<Integer> vcList = new ArrayList<>();
                        for (Integer vc : vcListIt) {
                            vcList.add(vc);
                        }
                        // 2.2 对List进行降序排序
                        vcList.sort((o1, o2) -> o2 - o1);
                        // 2.3 只保留最大的3个(list中的个数一定是连续变大，一超过3就立即清理即可)
                        if (vcList.size() > 3) {
                            // 将最后一个元素清除（第4个）
                            vcList.remove(3);
                        }
                        out.collect("传感器id为" + value.f0 + ",最大的3个水位值=" + vcList.toString());
                        // 3.更新list状态
                        listState.update(vcList);


//                                vcListState.get();            //取出 list状态 本组的数据，是一个Iterable
//                                vcListState.add();            // 向 list状态 本组 添加一个元素
//                                vcListState.addAll();         // 向 list状态 本组 添加多个元素
//                                vcListState.update();         // 更新 list状态 本组数据（覆盖）
//                                vcListState.clear();          // 清空List状态 本组数据
                    }
                });
        process.print();

        env.execute();

    }
}
