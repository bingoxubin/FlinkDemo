package com.bingoabin._03transformation._03UDF;

import com.bingoabin.pojo.Event;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author bingoabin
 * @date 2022/5/22 0:31
 */
public class LambdaTest1 {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		DataStreamSource<Event> clicks = env.fromElements(
				new Event("Mary", "./home", 1000L),
				new Event("Bob", "./cart", 2000L)
		                                                 );
		// flatMap 使用 Lambda 表达式，抛出异常
		//是对于像 flatMap() 这样的函数，它的函数签名 void flatMap(IN value, Collector<OUT>
		// out) 被 Java 编译器编译成了 void flatMap(IN value, Collector out)，也就是说将 Collector 的泛
		// 型信息擦除掉了。这样 Flink 就无法自动推断输出的类型信息了。
		// DataStream<String> stream2 = clicks.flatMap((event, out) -> {
		// 	out.collect(event.url);
		// });

		DataStream<String> stream2 = clicks.flatMap((Event event, Collector<String> out) -> {
			out.collect(event.url);
		}).returns(Types.STRING);
		stream2.print();

		env.execute();
	}
}
