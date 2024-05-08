package con.anqi.flink.aggreagte;

import con.anqi.flink.bean.WaterSensor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * keyby： 按照id分组
 * 要点：
 *  1、返回的是 一个 KeyedStream，键控流
 *  2、keyby不是 转换算子， 只是对数据进行重分区, 不能设置并行度
 *  3、分组 与 分区 的关系：
 *    1） keyby是对数据分组，保证 相同key的数据 在同一个分区（子任务）
 *    2） 分区： 一个子任务可以理解为一个分区，一个分区（子任务）中可以存在多个分组（key）
 */
public class KeyByDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<WaterSensor> sensorDS = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 11L, 11),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3)
        );
        // 按照 id 分组
        KeyedStream<WaterSensor, String> sensorKS = sensorDS
                .keyBy((KeySelector<WaterSensor, String>) WaterSensor::getId);

        sensorKS.print();
        env.execute();
    }
}
