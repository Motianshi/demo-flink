package con.anqi.flink.solution.join;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class InternalJoinDemo1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // A 流
        KeyedStream<Tuple2<String, String>, String> input1 = env.socketTextStream("", 9002)
                .map((MapFunction<String, Tuple2<String, String>>) s -> Tuple2.of(s.split(" ")[0], s.split(" ")[1]))
                .keyBy((KeySelector<Tuple2<String, String>, String>) tuple -> tuple.f0);

        // B 流
        KeyedStream<Tuple2<String, String>, String> input2 = env.socketTextStream("",9001)
                .map((MapFunction<String, Tuple2<String, String>>) s -> Tuple2.of(s.split(" ")[0],s.split(" ")[1]))
                .keyBy((KeySelector<Tuple2<String, String>, String>) tuple -> tuple.f0);
        SingleOutputStreamOperator<String> res = input1
                .intervalJoin(input2)
                // 定义 interval 的时间区间
                .between(Time.seconds(-30), Time.seconds(30))
                .process(new ProcessJoinFunction<Tuple2<String, String>, Tuple2<String, String>, String>() {
                    @Override
                    public void processElement(Tuple2<String, String> left, Tuple2<String, String> right,
                                               ProcessJoinFunction<Tuple2<String, String>, Tuple2<String, String>, String>.Context ctx, Collector<String> out) throws Exception {
                        out.collect(left.f0 + "->" + left.f1 + "," + right.f0 + "->" + right.f1);
                    }
                });
        res.print();
        env.execute();
    }
}
