package con.anqi.flink.window;

import con.anqi.flink.bean.WaterSensor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class WindowApiDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<WaterSensor> source = env.fromElements(
                new WaterSensor("1", 1L, 1),
                new WaterSensor("1", 2L, 2),
                new WaterSensor("2", 2L, 2),
                new WaterSensor("3", 3L, 3));
        KeyedStream<WaterSensor, String> keyedStream = source.keyBy((KeySelector<WaterSensor, String>) WaterSensor::getId);

        // TODO 1. 指定 窗口分配器： 指定 用 哪一种窗口 ---  时间 or 计数？ 滚动、滑动、会话？
        // 1.1 没有keyby的窗口: 窗口内的 所有数据 进入同一个 子任务，并行度只能为1
        //keyedStream.windowAll()
        // 1.2 有keyby的窗口: 每个key上都定义了一组窗口，各自独立地进行统计计算
        // 基于时间的
        //keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(10))) // 滚动窗口，窗口长度10s
        //keyedStream.window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(2))) // 滑动窗口，窗口长度10s，滑动步长2s
        //keyedStream.window(ProcessingTimeSessionWindows.withGap(Time.seconds(5))) // 会话窗口，超时间隔5s
        // 基于计数的
        //keyedStream.countWindow(5)  // 滚动窗口，窗口长度=5个元素
        //keyedStream.countWindow(5,2) // 滑动窗口，窗口长度=5个元素，滑动步长=2个元素
        //keyedStream.window(GlobalWindows.create())  // 全局窗口，计数窗口的底层就是用的这个，需要自定义的时候才会用
        // TODO 2. 指定 窗口函数 ： 窗口内数据的 计算逻辑
        WindowedStream<WaterSensor, String, TimeWindow> sensorWS = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));
        // 2.1增量聚合： 来一条数据，计算一条数据，窗口触发的时候输出计算结果
        //sensorWS
                //.reduce()
                //.aggregate(, )
         //2.2全窗口函数：数据来了不计算，存起来，窗口触发的时候，计算并输出结果
        //sensorWS.process()
        env.execute();
    }
}
