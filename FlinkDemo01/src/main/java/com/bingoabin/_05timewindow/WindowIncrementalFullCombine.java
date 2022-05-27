package com.bingoabin._05timewindow;

import com.bingoabin._02source.ClickSource;
import com.bingoabin.pojo.Event;
import com.bingoabin.pojo.UrlViewCount;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author bingoabin
 * @date 2022/5/22 21:27
 */
public class WindowIncrementalFullCombine {
	//增量聚合和全窗口函数的结合使用
	// ReduceFunction 与 WindowFunction 结合
	// public <R> SingleOutputStreamOperator<R> reduce(ReduceFunction<T> reduceFunction, WindowFunction<T, R, K, W> function)
	// ReduceFunction 与 ProcessWindowFunction 结合
	// public <R> SingleOutputStreamOperator<R> reduce(ReduceFunction<T> reduceFunction, ProcessWindowFunction<T, R, K, W> function)
	// AggregateFunction 与 WindowFunction 结合
	// public <ACC, V, R> SingleOutputStreamOperator<R> aggregate(AggregateFunction<T, ACC, V> aggFunction, WindowFunction<V, R, K, W> windowFunction)
	// AggregateFunction 与 ProcessWindowFunction 结合
	// public <ACC, V, R> SingleOutputStreamOperator<R> aggregate(AggregateFunction<T, ACC, V> aggFunction, ProcessWindowFunction<V, R, K, W> windowFunction)

	//这样调用的处理机制是：基于第一个参数（增量聚合函数）来处理窗口数据，每来一个数
	// 据就做一次聚合；等到窗口需要触发计算时，则调用第二个参数（全窗口函数）的处理逻辑输
	// 出结果。需要注意的是，这里的全窗口函数就不再缓存所有数据了，而是直接将增量聚合函数
	// 的结果拿来当作了 Iterable 类型的输入。一般情况下，这时的可迭代集合中就只有一个元素了。
	// 下面我们举一个具体的实例来说明。在网站的各种统计指标中，一个很重要的统计指标就 是热门的链接；
	// 想要得到热门的 url，前提是得到每个链接的“热门度”。一般情况下，可以用 url 的浏览量（点击量）表示热门度。
	// 我们这里统计 10 秒钟的 url 浏览量，每 5 秒钟更新一次； 另外为了更加清晰地展示，还应该把窗口的起始结束时间一起输出。
	// 我们可以定义滑动窗口， 并结合增量聚合函数和全窗口函数来得到统计结果。

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		SingleOutputStreamOperator<Event> stream = env
				.addSource(new ClickSource())
				.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
				                                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
					                                                @Override
					                                                public long extractTimestamp(Event element, long recordTimestamp) {
						                                                return element.timestamp;
					                                                }
				                                                }));
		// 需要按照 url 分组，开滑动窗口统计
		stream.keyBy(data -> data.url)
		      .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
		      // 同时传入增量聚合函数和全窗口函数
		      .aggregate(new UrlViewCountAgg(), new UrlViewCountResult())
		      .print();
		env.execute();
	}

	// 自定义增量聚合函数，来一条数据就加一
	public static class UrlViewCountAgg implements AggregateFunction<Event, Long, Long> {
		@Override
		public Long createAccumulator() {
			return 0L;
		}

		@Override
		public Long add(Event value, Long accumulator) {
			return accumulator + 1;
		}

		@Override
		public Long getResult(Long accumulator) {
			return accumulator;
		}

		@Override
		public Long merge(Long a, Long b) {
			return null;
		}
	}

	// 自定义窗口处理函数，只需要包装窗口信息
	public static class UrlViewCountResult extends ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow> {
		@Override
		public void process(String url, Context context, Iterable<Long> elements, Collector<UrlViewCount> out) throws Exception {
			// 结合窗口信息，包装输出内容
			Long start = context.window().getStart();
			Long end = context.window().getEnd();
			// 迭代器中只有一个元素，就是增量聚合函数的计算结果
			out.collect(new UrlViewCount(url, elements.iterator().next(), start, end));
		}
	}
}
