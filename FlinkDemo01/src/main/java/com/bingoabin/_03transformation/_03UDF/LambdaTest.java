package com.bingoabin._03transformation._03UDF;

import com.bingoabin.pojo.Event;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author bingoabin
 * @date 2022/5/22 0:29
 */
public class LambdaTest {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		DataStreamSource<Event> clicks = env.fromElements(
				new Event("Mary", "./home", 1000L),
				new Event("Bob", "./cart", 2000L)
		                                                 );
		//map 函数使用 Lambda 表达式，返回简单类型，不需要进行类型声明
		DataStream<String> stream1 = clicks.map(event -> event.url);
		stream1.print();

		env.execute();
	}
}
