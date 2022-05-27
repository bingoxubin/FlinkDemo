package com.bingoabin._08multistream;

/**
 * @author bingoabin
 * @date 2022/5/22 22:29
 */

import com.bingoabin._02source.ClickSource;
import com.bingoabin.pojo.Event;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SplitStreamByFilter {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource());
		// 筛选 Mary 的浏览行为放入 MaryStream 流中
		DataStream<Event> MaryStream = stream.filter(new FilterFunction<Event>() {
			@Override
			public boolean filter(Event value) throws Exception {
				return value.user.equals("Mary");
			}
		});
		// 筛选 Bob 的购买行为放入 BobStream 流中
		DataStream<Event> BobStream = stream.filter(new FilterFunction<Event>() {
			@Override
			public boolean filter(Event value) throws Exception {
				return value.user.equals("Bob");
			}
		});
		// 筛选其他人的浏览行为放入 elseStream 流中
		DataStream<Event> elseStream = stream.filter(new FilterFunction<Event>() {
			@Override
			public boolean filter(Event value) throws Exception {
				return !value.user.equals("Mary") && !value.user.equals("Bob");
			}
		});
		MaryStream.print("Mary pv");
		BobStream.print("Bob pv");
		elseStream.print("else pv");
		env.execute();
	}

	//在 Flink 1.13 版本中，已经弃用了.split()方法，取而代之的是直接用处理函数（process function）的侧输出流（side output）。
}
