package com.bingoabin._01wordcount;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @author bingoabin
 * @date 2022/5/21 14:21
 */
public class WordCountDataStream {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStreamSource<String> ds = env.readTextFile("FlinkDemo01/input/words.txt");
		SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOne = ds.flatMap((String lines, Collector<String> words) -> {
			                                                                Arrays.stream(lines.split(" ")).forEach(words::collect);
		                                                                }).returns(Types.STRING)
		                                                                .map(word -> Tuple2.of(word, 1L))
		                                                                .returns(Types.TUPLE(Types.STRING, Types.LONG));
		KeyedStream<Tuple2<String, Long>, String> wordAndOneKS = wordAndOne.keyBy(t -> t.f0);
		SingleOutputStreamOperator<Tuple2<String, Long>> rs = wordAndOneKS.sum(1);
		rs.print();
		env.execute();
	}
}
