package con.anqi.flink.sideoutput;

import con.anqi.flink.bean.WaterSensor;
import con.anqi.flink.functions.WaterSensorMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 使用侧输出流 实现分流
 * 需求： watersensor的数据，s1、s2的数据分别分开
 *
 * 总结步骤：
 *    1、使用 process算子
 *    2、定义 OutputTag对象
 *    3、调用 ctx.output
 *    4、通过主流 获取 测流
 */
public class SideOutputDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("66666", 7777)
                .map(new WaterSensorMapFunction());

        /**
         * 创建OutputTag对象
         * 第一个参数： 标签名
         * 第二个参数： 放入侧输出流中的 数据的 类型，Typeinformation
         */
        OutputTag<WaterSensor> s1Tag = new OutputTag<>("s1", Types.POJO(WaterSensor.class));
        OutputTag<WaterSensor> s2Tag = new OutputTag<>("s2", Types.POJO(WaterSensor.class));

        SingleOutputStreamOperator<WaterSensor> process = sensorDS
                .process(
                        new ProcessFunction<WaterSensor, WaterSensor>() {
                            @Override
                            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                                String id = value.getId();
                                if ("s1".equals(id)) {
                                    ctx.output(s1Tag, value); // 如果是 s1，放到侧输出流s1中
                                } else if ("s2".equals(id)) {
                                    ctx.output(s2Tag, value); // 如果是 s2，放到侧输出流s2中
                                } else {
                                    out.collect(value); // 非s1、s2的数据，放到主流中
                                }
                            }
                        }
                );
        // 从主流中，根据标签 获取 侧输出流
        SideOutputDataStream<WaterSensor> s1 = process.getSideOutput(s1Tag);
        SideOutputDataStream<WaterSensor> s2 = process.getSideOutput(s2Tag);

        // 打印主流
        process.print("主流-非s1、s2");

        //打印 侧输出流
        s1.printToErr("s1");
        s2.printToErr("s2");

        env.execute();
    }
}
