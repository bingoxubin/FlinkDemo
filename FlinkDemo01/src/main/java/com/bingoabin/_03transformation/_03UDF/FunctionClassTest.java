package com.bingoabin._03transformation._03UDF;

import com.bingoabin.pojo.Event;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author bingoabin
 * @date 2022/5/22 0:23
 */
public class FunctionClassTest {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		DataStreamSource<Event> clicks = env.fromElements(
				new Event("Mary", "./home", 1000L),
				new Event("Bob", "./cart", 2000L)
		                                                 );
		//方式一：
		// DataStream<Event> stream = clicks.filter(new FlinkFilter());

		//方式二：
		// DataStream<Event> stream = clicks.filter(new FilterFunction<Event>() {
		// 	@Override
		// 	public boolean filter(Event value) throws Exception {
		// 		return value.url.contains("home");
		// 	}
		// });

		//方式三：
		DataStream<Event> stream = clicks.filter(new FlinkFilter1("home"));

		stream.print();
		env.execute();
	}

	//方式一的
	public static class FlinkFilter implements FilterFunction<Event> {
		@Override
		public boolean filter(Event value) throws Exception {
			return value.url.contains("home");
		}
	}

	//方式三的
	public static class FlinkFilter1 implements FilterFunction<Event> {
		private String keyWord;

		FlinkFilter1(String keyWord) {
			this.keyWord = keyWord;
		}

		@Override
		public boolean filter(Event value) throws Exception {
			return value.url.contains(keyWord);
		}
	}
}
