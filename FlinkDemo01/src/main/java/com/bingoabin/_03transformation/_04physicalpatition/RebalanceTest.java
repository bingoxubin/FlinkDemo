package com.bingoabin._03transformation._04physicalpatition;

import com.bingoabin._02source.ClickSource;
import com.bingoabin.pojo.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author bingoabin
 * @date 2022/5/22 11:28
 */
public class RebalanceTest {
	//轮询分区（Round-Robin）
	public static void main(String[] args) throws Exception {
		// 创建执行环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		// 读取数据源，并行度为 1
		DataStreamSource<Event> stream = env.addSource(new ClickSource());
		// 经轮询重分区后打印输出，并行度为 4
		stream.rebalance().print("rebalance").setParallelism(4);
		env.execute();
	}
}
