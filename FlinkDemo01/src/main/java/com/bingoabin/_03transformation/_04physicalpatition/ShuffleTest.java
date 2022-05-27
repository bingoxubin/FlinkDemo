package com.bingoabin._03transformation._04physicalpatition;

import com.bingoabin._02source.ClickSource;
import com.bingoabin.pojo.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author bingoabin
 * @date 2022/5/22 11:22
 */
public class ShuffleTest {
	//随机分区（shuffle）
	public static void main(String[] args) throws Exception{
	    //物理分区与 keyBy 另一大区别在于，keyBy 之后得到的是一个 KeyedStream，而物理分
		// 区之后结果仍是 DataStream，且流中元素数据类型保持不变。从这一点也可以看出，分区算子
		// 并不对数据进行转换处理，只是定义了数据的传输方式。

		//物理分区策略有随机分配（Random）、轮询分配（Round-Robin）、重缩放（Rescale）和广播（Broadcast）
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		DataStreamSource<Event> stream = env.addSource(new ClickSource());

		stream.shuffle().print("shuffle").setParallelism(4);

		env.execute();
	}
}
