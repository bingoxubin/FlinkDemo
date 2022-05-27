package com.bingoabin._03transformation._01basic;

import com.bingoabin.pojo.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author bingoabin
 * @date 2022/5/21 23:07
 */
public class MapTest {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		DataStreamSource<Event> events = env.fromElements(
				new Event("Marry", "./home", 1000L),
				new Event("Bob", "./cart", 2000L)
		                                                                );
		events.map(new UserExtractor()).print();
		env.execute();
	}

	public static class UserExtractor implements MapFunction<Event, String> {

		@Override
		public String map(Event event) throws Exception {
			return event.user;
		}
	}
}
