package con.anqi.flink.join;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class InternalJoinDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> source1 = env.fromElements(
                "a,1", "a,2", "a,4", "a,3", "b,2", "b,1", "b,9", "e,1");

        DataStreamSource<String> source2 = env.fromElements(
                "a,1,11", "a,3,2", "a,4,2", "b,3,7", "b,3,8", "b,19,1", "e,9,1", "e,1,1");


        KeyedStream<Tuple2<String, Long>, String> stream1 = source1
                .flatMap((FlatMapFunction<String, Tuple2<String, Long>>) (str, collector) -> {
                    String[] split = str.split(",");
                    collector.collect(Tuple2.of(split[0], Long.parseLong(split[1])));
                })
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((SerializableTimestampAssigner<Tuple2<String, Long>>) (tuple, recordTime) -> tuple.f1 * 1000L)
                )
                .keyBy(k -> k.f0);

        KeyedStream<Tuple3<String, Long, String>, String> stream2 = source2
            .flatMap((FlatMapFunction<String, Tuple3<String, Long, String>>) (str, collector) -> {
                String[] split = str.split(",");
                collector.collect(Tuple3.of(split[0], Long.parseLong(split[1]), split[2]));
            })
            .returns(Types.TUPLE(Types.STRING, Types.LONG, Types.STRING))
            .assignTimestampsAndWatermarks(WatermarkStrategy
                    .<Tuple3<String, Long, String>>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                    .withTimestampAssigner((SerializableTimestampAssigner<Tuple3<String, Long, String>>) (tuple, recordTime) -> tuple.f1 * 1000L)
            )
            .keyBy(k -> k.f0);

        OutputTag<Tuple2<String, Long>> leftTag = new OutputTag<>("leftTag", Types.TUPLE(Types.STRING, Types.LONG));
        OutputTag<Tuple3<String, Long, String>> rightTag = new OutputTag<>("rightTag", Types.TUPLE(Types.STRING, Types.LONG, Types.STRING));

        SingleOutputStreamOperator<String> process = stream1.intervalJoin(stream2)
                .between(Time.seconds(-1), Time.seconds(1))
                .sideOutputLeftLateData(leftTag)
                .sideOutputRightLateData(rightTag)
                .process(new ProcessJoinFunction<Tuple2<String, Long>, Tuple3<String, Long, String>, String>() {
                    @Override
                    public void processElement(Tuple2<String, Long> left, Tuple3<String, Long, String> right, ProcessJoinFunction<Tuple2<String, Long>, Tuple3<String, Long, String>, String>.Context ctx, Collector<String> out) throws Exception {
                        out.collect(left + "->" + right);
                    }
                });
        process.print("主流");
        process.getSideOutput(leftTag).print();
        process.getSideOutput(rightTag).print();

        env.execute();

    }
}
