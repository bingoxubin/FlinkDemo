package com.bingoabin._03transformation._02Aggregation;

import com.bingoabin.pojo.Event;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author bingoabin
 * @date 2022/5/21 23:43
 */
public class KeyByTest {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		DataStreamSource<Event> stream = env.fromElements(
				new Event("Marry", "./home", 1000L),
				new Event("Bob", "./cart", 2000L)
		                                                                );

		//方式一：
		// KeyedStream<Event, String> keyStream = stream.keyBy(e -> e.user);

		//方式二：
		KeyedStream<Event, String> keyStream = stream.keyBy(new KeySelector<Event, String>() {
			@Override
			public String getKey(Event event) throws Exception {
				return event.user;
			}
		});

		//keyBy 得到的结果将不再是 DataStream，而是会将 DataStream 转换为
		// KeyedStream。KeyedStream 可以认为是“分区流”或者“键控流”，它是对 DataStream 按照
		// key 的一个逻辑分区，所以泛型有两个类型：除去当前流中的元素类型外，还需要指定 key 的
		// 类型
		// keyStream.print();
		keyStream.writeAsText("FlinkDemo01/output/file.txt");
		env.execute();
	}
}
