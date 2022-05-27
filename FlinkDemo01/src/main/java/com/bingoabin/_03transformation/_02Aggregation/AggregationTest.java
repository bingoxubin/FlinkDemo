package com.bingoabin._03transformation._02Aggregation;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author bingoabin
 * @date 2022/5/21 23:55
 */
public class AggregationTest {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		DataStreamSource<Tuple3<String, Integer, String>> stream = env.fromElements(Tuple3.of("a", 3, "b"), Tuple3.of("a", 1, "c"), Tuple3.of("b", 3, "d"), Tuple3.of("b", 4, "e"));
		// stream.keyBy(r -> r.f0).sum(1).print();
		// stream.keyBy(r -> r.f0).sum("f1").print();
		// stream.keyBy(r -> r.f0).max(1).print();
		// stream.keyBy(r -> r.f0).max("f1").print();
		// stream.keyBy(r -> r.f0).min(1).print();
		// stream.keyBy(r -> r.f0).min("f1").print();
		// stream.keyBy(r -> r.f0).maxBy(1).print();
		// stream.keyBy(r -> r.f0).maxBy("f1").print();
		stream.keyBy(r -> r.f0).minBy(1).print();
		// stream.keyBy(r -> r.f0).minBy("f1").print();
		env.execute();

		//min():只求了最小的值，其他字段是第一条记录的值
		//(a,3,b)
		// (a,1,b)
		// (b,3,d)
		// (b,3,d)

		//minby()：求了最小值，其他字段就是取到最小值的这条记录的其他值
		//(a,3,b)
		// (a,1,c)
		// (b,3,d)
		// (b,3,d)
	}
}
