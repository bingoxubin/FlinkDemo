package com.bingoabin._02source;

import com.bingoabin.pojo.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * @author bingoabin
 * @date 2022/5/21 20:43
 */
public class ReadFromList {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		ArrayList<Event> events = new ArrayList<>();
		events.add(new Event("marry", "./home", 1000L));
		events.add(new Event("bob", "./cart", 1000L));

		DataStreamSource<Event> ds = env.fromCollection(events);
		ds.print();
		env.execute();
	}
}
