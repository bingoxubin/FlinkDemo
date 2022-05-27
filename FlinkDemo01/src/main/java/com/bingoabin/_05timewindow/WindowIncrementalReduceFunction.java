package com.bingoabin._05timewindow;

import com.bingoabin._02source.ClickSource;
import com.bingoabin.pojo.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * @author bingoabin
 * @date 2022/5/22 20:45
 */
public class WindowIncrementalReduceFunction {
	//典型的增量聚合函数有两个：ReduceFunction 和 AggregateFunction。
	//增量聚合函数（incremental aggregation functions）  归约函数（ReduceFunction）
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		// 从自定义数据源读取数据，并提取时间戳、生成水位线
		SingleOutputStreamOperator<Event> stream = env
				.addSource(new ClickSource())
				.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
				                                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
					                                                @Override
					                                                public long extractTimestamp(Event event, long l) {
						                                                return event.timestamp;
					                                                }
				                                                }));
		stream.map(new MapFunction<Event, Tuple2<String, Long>>() {
			      @Override
			      public Tuple2<String, Long> map(Event event) throws Exception {
				      // 将数据转换成二元组，方便计算
				      return Tuple2.of(event.user, 1L);
			      }
		      })
		      .keyBy(r -> r.f0)
		      // 设置滚动事件时间窗口
		      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
		      .reduce(new ReduceFunction<Tuple2<String, Long>>() {
			      @Override
			      public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
				      // 定义累加规则，窗口闭合时，向下游发送累加结果
					  return Tuple2.of(value1.f0, value1.f1 + value2.f1);
			      }
		      })
		      .print();
		env.execute();

		//对于窗口的计算，我们用 ReduceFunction 对 count 值做了增量聚合：窗口中会将当前的总 count
		// 值保存成一个归约状态，每来一条数据，就会调用内部的 reduce 方法，将新数据中的 count
		// 值叠加到状态上，并得到新的状态保存起来。等到了 5 秒窗口的结束时间，就把归约好的状态
		// 直接输出。

		//注意：ReduceFunction 可以解决大多数归约聚合的问题，但是这个接口有一个限制，就是聚合状
		// 态的类型、输出结果的类型都必须和输入数据类型一样。这就迫使我们必须在聚合前，先将数
		// 据转换（map）成预期结果类型；而在有些情况下，还需要对状态进行进一步处理才能得到输
		// 出结果，这时它们的类型可能不同，使用 ReduceFunction 就会非常麻烦。
		//例如，如果我们希望计算一组数据的平均值，应该怎样做聚合呢？很明显，这时我们需要
		// 计算两个状态量：数据的总和（sum），以及数据的个数（count），而最终输出结果是两者的商
		// （sum/count）。如果用 ReduceFunction，那么我们应该先把数据转换成二元组(sum, count)的形
		// 式，然后进行归约聚合，最后再将元组的两个元素相除转换得到最后的平均值。本来应该只是
		// 一个任务，可我们却需要 map-reduce-map 三步操作，这显然不够高效。
	}
}
