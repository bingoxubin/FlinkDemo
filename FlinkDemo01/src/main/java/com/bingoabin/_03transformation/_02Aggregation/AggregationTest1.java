package com.bingoabin._03transformation._02Aggregation;

import com.bingoabin.pojo.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author bingoabin
 * @date 2022/5/22 0:04
 */
public class AggregationTest1 {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		DataStreamSource<Event> stream = env.fromElements(
				new Event("Mary", "./home", 1000L),
				new Event("Bob", "./cart", 2000L),
				new Event("Bob", "./cart", 3000L),
				new Event("Bob", "./cart", 2000L),
				new Event("Bob", "./cart", 2000L),
				new Event("Bob", "./cart", 2000L)
		                                                 );
		//如果是pojo类，只能通过字段名来指定求最大最小值了，如果是tuple可以使用f0,f1等
		// stream.keyBy(e -> e.user).max("timestamp").print(); // 指定字段名称
		stream.keyBy(e -> e.user).max("timestamp").writeAsText("FlinkDemo01/output/file.txt");
		env.execute();
	}
}
