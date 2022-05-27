package com.bingoabin._05timewindow;

import com.bingoabin._02source.ClickSource;
import com.bingoabin.pojo.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.HashSet;

/**
 * @author bingoabin
 * @date 2022/5/22 20:56
 */
public class WindowIncrementalAggregateFunction {
	//典型的增量聚合函数有两个：ReduceFunction 和 AggregateFunction。
	//增量聚合函数（incremental aggregation functions）  聚合函数（AggregateFunction）

	//AggregateFunction 可以看作是 ReduceFunction 的通用版本，这里有三种类型：输入类型
	// （IN）、累加器类型（ACC）和输出类型（OUT）。输入类型 IN 就是输入流中元素的数据类型；
	// 累加器类型 ACC 则是我们进行聚合的中间状态类型；而输出类型当然就是最终计算结果的类型了。
	// 接口中有四个方法：
	// ⚫ createAccumulator()：创建一个累加器，这就是为聚合创建了一个初始状态，每个聚
	// 合任务只会调用一次。
	// ⚫ add()：将输入的元素添加到累加器中。这就是基于聚合状态，对新来的数据进行进
	// 一步聚合的过程。方法传入两个参数：当前新到的数据 value，和当前的累加器
	// accumulator；返回一个新的累加器值，也就是对聚合状态进行更新。每条数据到来之
	// 后都会调用这个方法。
	// ⚫ getResult()：从累加器中提取聚合的输出结果。也就是说，我们可以定义多个状态，
	// 然后再基于这些聚合的状态计算出一个结果进行输出。比如之前我们提到的计算平均
	// 值，就可以把 sum 和 count 作为状态放入累加器，而在调用这个方法时相除得到最终
	// 结果。这个方法只在窗口要输出结果时调用。
	// ⚫ merge()：合并两个累加器，并将合并后的状态作为一个累加器返回。这个方法只在
	// 需要合并窗口的场景下才会被调用；最常见的合并窗口（Merging Window）的场景
	// 就是会话窗口（Session Windows）。

	//需求：在电商网站中，PV（页面浏览量）和 UV（独立访客
	// 数）是非常重要的两个流量指标。一般来说，PV 统计的是所有的点击量；而对用户 id 进行去
	// 重之后，得到的就是 UV。所以有时我们会用 PV/UV 这个比值，来表示“人均重复访问量”，
	// 也就是平均每个用户会访问多少次页面，这在一定程度上代表了用户的粘度。
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

		// 所有数据设置相同的 key，发送到同一个分区统计 PV 和 UV，再相除
		stream.keyBy(data -> true)
		      .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2)))
		      .aggregate((new AvgPv()))
		      .print();

		env.execute();
	}

	public static class AvgPv implements AggregateFunction<Event, Tuple2<HashSet<String>, Long>, Double> {

		@Override
		public Tuple2<HashSet<String>, Long> createAccumulator() {
			// 创建累加器
			return Tuple2.of(new HashSet<String>(), 0L);
		}

		@Override
		public Tuple2<HashSet<String>, Long> add(Event value, Tuple2<HashSet<String>, Long> accumulator) {
			// 属于本窗口的数据来一条累加一次，并返回累加器
			accumulator.f0.add(value.user);
			return Tuple2.of(accumulator.f0, accumulator.f1 + 1L);
		}

		@Override
		public Double getResult(Tuple2<HashSet<String>, Long> accumulator) {
			// 窗口闭合时，增量聚合结束，将计算结果发送到下游
			return (double) accumulator.f1 / accumulator.f0.size();
		}

		@Override
		public Tuple2<HashSet<String>, Long> merge(Tuple2<HashSet<String>, Long> hashSetLongTuple2, Tuple2<HashSet<String>, Long> acc1) {
			return null;
		}
	}
}
