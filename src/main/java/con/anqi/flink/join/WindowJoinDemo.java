package con.anqi.flink.join;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class WindowJoinDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Tuple2<String, Long>> source1 = env.fromElements(
                Tuple2.of("a", 1L), Tuple2.of("a", 2L), Tuple2.of("a", 4L), Tuple2.of("a", 3L),
                Tuple2.of("b", 2L), Tuple2.of("b", 1L), Tuple2.of("b", 9L), Tuple2.of("e", 1L));

        DataStreamSource<Tuple3<String, Long, String>> source2 = env.fromElements(
                Tuple3.of("a", 1L, "11"), Tuple3.of("a", 3L, "2"), Tuple3.of("a", 4L, "2"),
                Tuple3.of("b", 3L, "7"), Tuple3.of("b", 3L, "8"), Tuple3.of("b", 19L, "1"),
                Tuple3.of("e", 9L, "1"), Tuple3.of("e", 1L, "1")
        );

        SingleOutputStreamOperator<Tuple2<String, Long>> str1 = source1
            .assignTimestampsAndWatermarks(WatermarkStrategy
                    .<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                    .withTimestampAssigner((SerializableTimestampAssigner<Tuple2<String, Long>>) (tuple, recordTime) -> tuple.f1 * 1000L)
            );

        SingleOutputStreamOperator<Tuple3<String, Long, String>> str2 = source2
            .assignTimestampsAndWatermarks(WatermarkStrategy
                    .<Tuple3<String, Long, String>>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                    .withTimestampAssigner((SerializableTimestampAssigner<Tuple3<String, Long, String>>) (tuple, recordTime) -> tuple.f1 * 1000L)
            );

        str1.join(str2)
                .where((KeySelector<Tuple2<String, Long>, String>) tuple -> tuple.f0)
                .equalTo((KeySelector<Tuple3<String, Long, String>, String>) tuple -> tuple.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply((JoinFunction<Tuple2<String, Long>, Tuple3<String, Long, String>, String>) (v1, v2) -> v1 + "->" + v2)
                .print();
        env.execute();
    }
}
