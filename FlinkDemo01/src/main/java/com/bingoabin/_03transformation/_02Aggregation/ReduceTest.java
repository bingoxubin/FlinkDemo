package com.bingoabin._03transformation._02Aggregation;

import com.bingoabin._02source.ClickSource;
import com.bingoabin.pojo.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author bingoabin
 * @date 2022/5/22 0:07
 */
public class ReduceTest {
	//我们将数据流按照用户 id 进行分区，然后用一个 reduce 算子实现 sum 的功能，统计每个
	// 用户访问的频次；进而将所有统计结果分到一组，用另一个 reduce 算子实现 maxBy 的功能，
	// 记录所有用户中访问频次最高的那个，也就是当前访问量最大的用户是谁。
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		// 这里的 ClickSource()使用了之前自定义数据源小节中的 ClickSource()
		env.addSource(new ClickSource())
		   // 将 Event 数据类型转换成元组类型
		   .map(new MapFunction<Event, Tuple2<String, Long>>() {
			   @Override
			   public Tuple2<String, Long> map(Event e) throws Exception {
				   return Tuple2.of(e.user, 1L);
			   }
		   }).keyBy(r -> r.f0) // 使用用户名来进行分流
		   .reduce(new ReduceFunction<Tuple2<String, Long>>() {
			   @Override
			   public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
				   // 每到一条数据，用户 pv 的统计值加 1
				   return Tuple2.of(value1.f0, value1.f1 + value2.f1);
			   }
		   }).keyBy(r -> true) // 为每一条数据分配同一个 key，将聚合结果发送到一条流中去
		   .reduce(new ReduceFunction<Tuple2<String, Long>>() {
			   @Override
			   public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
				   // 将累加器更新为当前最大的 pv 统计值，然后向下游发送累加器的值
				   return value1.f1 > value2.f1 ? value1 : value2;
			   }
		   }).print();
		env.execute();
	}
}
