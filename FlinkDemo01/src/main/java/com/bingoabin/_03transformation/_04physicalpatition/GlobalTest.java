package com.bingoabin._03transformation._04physicalpatition;

import com.bingoabin._02source.ClickSource;
import com.bingoabin.pojo.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author bingoabin
 * @date 2022/5/22 11:37
 */
public class GlobalTest {
	//全局分区（global）
	//全局分区也是一种特殊的分区方式。这种做法非常极端，通过调用.global()方法，会将所
	// 有的输入流数据都发送到下游算子的第一个并行子任务中去。这就相当于强行让下游任务并行
	// 度变成了 1，所以使用这个操作需要非常谨慎，可能对程序造成很大的压力。
	public static void main(String[] args) throws Exception {
		// 创建执行环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		// 读取数据源，并行度为 1
		DataStreamSource<Event> stream = env.addSource(new ClickSource());
		// 经轮询重分区后打印输出，并行度为 4
		stream.global().print("rebalance").setParallelism(4);
		env.execute();
	}
}
