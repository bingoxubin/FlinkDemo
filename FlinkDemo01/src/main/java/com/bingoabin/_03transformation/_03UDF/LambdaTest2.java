package com.bingoabin._03transformation._03UDF;

import com.bingoabin.pojo.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author bingoabin
 * @date 2022/5/22 0:37
 */
public class LambdaTest2 {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		DataStreamSource<Event> clicks = env.fromElements(
				new Event("Mary", "./home", 1000L),
				new Event("Bob", "./cart", 2000L)
		                                                 );

		//使用 map 函数也会出现类似问题，以下代码会报错
		//当使用 map() 函数返回 Flink 自定义的元组类型时也会发生类似的问题。下例中的函数签
		// 名 Tuple2<String, Long> map(Event value) 被类型擦除为 Tuple2 map(Event value)。
		// DataStream<Tuple2<String, Long>> stream3 = clicks
		// 		.map( event -> Tuple2.of(event.user, 1L) );

		//方式一：
		// DataStream<Tuple2<String, Long>> stream3 = clicks
		// 		.map(event -> Tuple2.of(event.user, 1L))
		// 		.returns(Types.TUPLE(Types.STRING, Types.LONG));

		//方式二：
		// DataStream<Tuple2<String, Long>> stream3 = clicks.map(new MapFunction<Event, Tuple2<String, Long>>() {
		// 	@Override
		// 	public Tuple2<String, Long> map(Event event) throws Exception {
		// 		return Tuple2.of(event.user, 1L);
		// 	}
		// });

		//方式三：
		DataStream<Tuple2<String, Long>> stream3 = clicks.map(new MyMapFunction());

		stream3.print();

		env.execute();
	}

	public static class MyMapFunction implements MapFunction<Event, Tuple2<String, Long>> {
		@Override
		public Tuple2<String, Long> map(Event event) throws Exception {
			return Tuple2.of(event.user, 1L);
		}
	}
}
