package com.bingoabin._03transformation._01basic;

import com.bingoabin.pojo.Event;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author bingoabin
 * @date 2022/5/21 23:19
 */
public class FilterTest {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		DataStreamSource<Event> events = env.fromElements(
				new Event("Marry", "./home", 1000L),
				new Event("Bob", "./cart", 2000L)
		                                                 );
		//方式一：
		// events.filter(new UserFilter()).print();
		//方式二：
		events.filter(new FilterFunction<Event>() {
			@Override
			public boolean filter(Event event) throws Exception {
				return event.user.equals("Marry");
			}
		}).print();
		env.execute();
	}

	public static class UserFilter implements FilterFunction<Event> {

		@Override
		public boolean filter(Event event) throws Exception {
			return event.user.equals("Marry");
		}
	}
}
