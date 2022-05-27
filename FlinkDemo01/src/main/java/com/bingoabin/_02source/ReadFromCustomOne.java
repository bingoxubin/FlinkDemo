package com.bingoabin._02source;

import com.bingoabin.pojo.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author bingoabin
 * @date 2022/5/21 22:41
 */
public class ReadFromCustomOne {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		DataStreamSource<Event> ds = env.addSource(new ClickSource());
		// 只能设置并行度为1，如果想要并行，需要使用ParallelSourceFunction
		// DataStreamSource<Event> ds = env.addSource(new ClickSource()).setParallelism(2);
		ds.print("SourceCustom");
		env.execute();
	}
}
