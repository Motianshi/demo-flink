package con.anqi.flink.topn;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class TopNDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Tuple3<String, Integer, String>> source1 = env.fromElements(
                Tuple3.of("/user", 1, "11"), Tuple3.of("/login", 3, "2"), Tuple3.of("/logout", 4, "2"),
                Tuple3.of("/user", 3, "7"), Tuple3.of("/login", 3, "8"), Tuple3.of("/modify", 19, "1"),
                Tuple3.of("/info", 9, "1"), Tuple3.of("/info", 1, "1")
        );

        SingleOutputStreamOperator<Tuple3<String, Integer, String>> stream = source1
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Tuple3<String, Integer, String>>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((SerializableTimestampAssigner<Tuple3<String, Integer, String>>) (tuple, recordTime) -> tuple.f1 * 1000L)
                );

        /**
         * TODO 思路二： 使用 KeyedProcessFunction实现
         * 1、按照vc做keyby，开窗，分别count
         *    ==》 增量聚合，计算 count
         *    ==》 全窗口，对计算结果 count值封装 ，  带上 窗口结束时间的 标签
         *          ==》 为了让同一个窗口时间范围的计算结果到一起去
         *
         * 2、对同一个窗口范围的count值进行处理： 排序、取前N个
         *    =》 按照 windowEnd做keyby
         *    =》 使用process， 来一条调用一次，需要先存，分开存，用HashMap,key=windowEnd,value=List
         *      =》 使用定时器，对 存起来的结果 进行 排序、取前N个
         */

        stream
                .keyBy((KeySelector<Tuple3<String, Integer, String>, String>) tuple -> tuple.f0)
                .window(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(3)))
                .aggregate(new CountAggFunc(), new CountProcessWindowFunc())
                .keyBy((KeySelector<Tuple4<String, Long, Long, Integer>, Long>) tuple -> tuple.f2)
                .process(new KeyedProcessFunction<Long, Tuple4<String, Long, Long, Integer>, String>() {
                    ListState<Tuple4<String, Long, Long, Integer>> urlCount;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        urlCount= getRuntimeContext().getListState(new ListStateDescriptor<>("urlCountList", Types.TUPLE(Types.LONG, Types.STRING, Types.INT)));

                    }

                    @Override
                    public void processElement(Tuple4<String, Long, Long, Integer> value, KeyedProcessFunction<Long, Tuple4<String, Long, Long, Integer>, String>.Context ctx, Collector<String> out) throws Exception {
                        urlCount.add(value);
                        ctx.timerService().registerEventTimeTimer(value.f2 + 1);
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<Long, Tuple4<String, Long, Long, Integer>, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        List<Tuple4<String, Long, Long, Integer>> urlCountList = new ArrayList<>();
                        for (Tuple4<String, Long, Long, Integer> tuple : urlCount.get()) {
                            urlCountList.add(tuple);
                        }
                        urlCount.clear();;
                        urlCountList.sort(Comparator.comparingInt(o -> o.f3));
                        StringBuilder outBuilder = new StringBuilder();
                        outBuilder.append("top3 url 信息如下：");
                        for (int i = 0; i < Math.min(3, urlCountList.size()); i++) {
                            outBuilder.append("No." + (i +1) + ": -> ");
                            outBuilder.append(urlCountList.get(i).f0 + ",访问次数:" + urlCountList.get(i).f3);
                            outBuilder.append("窗口:" + urlCountList.get(i).f1 + "-" + urlCountList.get(i).f2);
                        }
                        out.collect(outBuilder.toString());
                    }
                })
                .print();

        env.execute();

    }


    public static class CountAggFunc implements AggregateFunction<Tuple3<String, Integer, String>, Integer, Integer> {

        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(Tuple3<String, Integer, String> tuple, Integer acc) {
            return acc + 1;
        }

        @Override
        public Integer getResult(Integer acc) {
            return acc;
        }

        @Override
        public Integer merge(Integer integer, Integer acc) {
            return null;
        }
    }

    public static class CountProcessWindowFunc extends ProcessWindowFunction<Integer, Tuple4<String, Long, Long, Integer>, String, TimeWindow> {

        @Override
        public void process(String url, ProcessWindowFunction<Integer, Tuple4<String, Long, Long, Integer>, String, TimeWindow>.Context context, Iterable<Integer> elements, Collector<Tuple4<String, Long, Long, Integer>> out) throws Exception {
            Integer count = elements.iterator().next();
            Long start = context.window().getStart();
            Long end = context.window().getEnd();
            out.collect(Tuple4.of(url, start, end, count));
        }
    }
}
