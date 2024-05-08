package con.anqi.flink.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CollectionSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> source = env
                .fromElements(1,2,33); // 从元素读
//              .fromCollection(Arrays.asList(1, 22, 3));  // 从集合读
        source.print();
        env.execute();
    }
}
