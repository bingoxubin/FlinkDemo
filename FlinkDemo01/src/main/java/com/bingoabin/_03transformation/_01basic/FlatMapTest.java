package com.bingoabin._03transformation._01basic;

import com.bingoabin.pojo.Event;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author bingoabin
 * @date 2022/5/21 23:25
 */
public class FlatMapTest {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		DataStreamSource<Event> stream = env.fromElements(
				new Event("Marry", "./home", 1000L),
				new Event("Bob", "./cart", 2000L)
		                                                 );
		stream.flatMap(new MyFlatMap()).print();
		env.execute();
	}

	public static class MyFlatMap implements FlatMapFunction<Event, String> {

		@Override
		public void flatMap(Event event, Collector<String> collector) throws Exception {
			if (event.user.equals("Marry")) {
				collector.collect(event.user);
			} else if (event.user.equals("Bob")) {
				collector.collect(event.user);
				collector.collect(event.url);
			}
		}
	}
}
