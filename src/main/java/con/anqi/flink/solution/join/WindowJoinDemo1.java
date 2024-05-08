package con.anqi.flink.solution.join;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class WindowJoinDemo1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // A 流
        DataStream<Tuple2<String,String>> input1 = env.socketTextStream("",9002)
                .map((MapFunction<String, Tuple2<String, String>>) s -> Tuple2.of(s.split(" ")[0],s.split(" ")[1]));

        // B 流
        DataStream<Tuple2<String,String>> input2 = env.socketTextStream("",9001)
                .map((MapFunction<String, Tuple2<String, String>>) s -> Tuple2.of(s.split(" ")[0],s.split(" ")[1]));

        DataStream<String> res = input1
                .join(input2)
                .where((KeySelector<Tuple2<String, String>, String>) tuple -> tuple.f0)
                .equalTo((KeySelector<Tuple2<String, String>, String>) tuple -> tuple.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(60)))
                .apply(new JoinFunction<Tuple2<String, String>, Tuple2<String, String>, String>() {
                    //窗口中关联到的数据处理逻辑
                    @Override
                    public String join(Tuple2<String, String> tuple1, Tuple2<String, String> tuple2) throws Exception {
                        return null;
                    }
                });
        res.print();
        env.execute();
    }
}

//        DataStream<String> res = input1
//                .coGroup(input2)
//                .where((KeySelector<Tuple2<String, String>, String>) tuple -> tuple.f0)
//                .equalTo((KeySelector<Tuple2<String, String>, String>) tuple -> tuple.f0)
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(60)))
//                .apply(new CoGroupFunction<Tuple2<String, String>, Tuple2<String, String>, String>() {
//                    @Override
//                    public void coGroup(Iterable<Tuple2<String, String>> it1, Iterable<Tuple2<String, String>> it2, Collector<String> collector) throws Exception {
//                        StringBuffer builder =new StringBuffer();
//                        builder.append("DataStream frist:\n");
//                        for(Tuple2<String,String> value : it1){
//                            builder.append(value.f0+"=>"+value.f1+"\n");
//                        }
//                        builder.append("DataStream second:\n");
//                        for(Tuple2<String,String> value : it2){
//                            builder.append(value.f0+"=>"+value.f1+"\n");
//                        }
//                        collector.collect(builder.toString());
//                    }
//                });