package com.bingoabin.TopN;

/**
 * @author bingoabin
 * @date 2022/5/22 22:21
 */

import com.bingoabin._02source.ClickSource;
import com.bingoabin.pojo.Event;
import com.bingoabin.pojo.UrlViewCount;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;

//具体实现思路就是，先按照 url 对数据进行 keyBy 分区，然后开窗进行增量聚合。这里就
// 会发现一个问题：我们进行按键分区之后，窗口的计算就会只针对当前 key 有效了；也就是说，
// 每个窗口的统计结果中，只会有一个 url 的浏览量，这是无法直接用 ProcessWindowFunction
// 进行排序的。所以我们只能分成两步：先对每个 url 链接统计出浏览量，然后再将统计结果收
// 集起来，排序输出最终结果。因为最后的排序还是基于每个时间窗口的，所以为了让输出的统
// 计结果中包含窗口信息，我们可以借用第六章中定义的 POJO 类 UrlViewCount 来表示，它包
// 含了 url、浏览量（count）以及窗口的起始结束时间。之后对 UrlViewCount 的处理，可以先按
// 窗口分区，然后用 KeyedProcessFunction 来实现。
// 总结处理流程如下：
// （1）读取数据源；
// （2）筛选浏览行为（pv）；
// （3）提取时间戳并生成水位线；
// （4）按照 url 进行 keyBy 分区操作；
// （5）开长度为 1 小时、步长为 5 分钟的事件时间滑动窗口；
// （6）使用增量聚合函数 AggregateFunction，并结合全窗口函数 WindowFunction 进行窗口
// 聚合，得到每个 url、在每个统计窗口内的浏览量，包装成 UrlViewCount； （7）按照窗口进行 keyBy 分区操作；
// （8）对同一窗口的统计结果数据，使用 KeyedProcessFunction 进行收集并排序输出。
public class KeyedProcessTopN {
	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		// 从自定义数据源读取数据
		SingleOutputStreamOperator<Event> eventStream = env
				.addSource(new ClickSource())
				.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
				                                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
					                                                @Override
					                                                public long extractTimestamp(Event element, long
							                                                recordTimestamp) {
						                                                return element.timestamp;
					                                                }
				                                                }));
		// 需要按照 url 分组，求出每个 url 的访问量
		SingleOutputStreamOperator<UrlViewCount> urlCountStream =
				eventStream.keyBy(data -> data.url)
				           .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
				           .aggregate(new UrlViewCountAgg(), new UrlViewCountResult());
		// 对结果中同一个窗口的统计数据，进行排序处理
		SingleOutputStreamOperator<String> result = urlCountStream
				.keyBy(data -> data.windowEnd)
				.process(new TopN(2));
		result.print("result");
		env.execute();
	}

	// 自定义增量聚合
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

	// 自定义全窗口函数，只需要包装窗口信息
	public static class UrlViewCountResult extends ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow> {
		@Override
		public void process(String url, Context context, Iterable<Long> elements, Collector<UrlViewCount> out) throws Exception {
			// 结合窗口信息，包装输出内容
			Long start = context.window().getStart();
			Long end = context.window().getEnd();
			out.collect(new UrlViewCount(url, elements.iterator().next(), start, end));
		}
	}

	// 自定义处理函数，排序取 top n
	public static class TopN extends KeyedProcessFunction<Long, UrlViewCount, String> {
		// 将 n 作为属性
		private Integer n;
		// 定义一个列表状态
		private ListState<UrlViewCount> urlViewCountListState;

		public TopN(Integer n) {
			this.n = n;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			// 从环境中获取列表状态句柄
			urlViewCountListState = getRuntimeContext()
					.getListState(new ListStateDescriptor<UrlViewCount>("url-view-count-list", Types.POJO(UrlViewCount.class)));
		}

		@Override
		public void processElement(UrlViewCount value, Context ctx, Collector<String> out) throws Exception {
			// 将 count 数据添加到列表状态中，保存起来
			urlViewCountListState.add(value);
			// 注册 window end + 1ms 后的定时器，等待所有数据到齐开始排序
			ctx.timerService().registerEventTimeTimer(ctx.getCurrentKey() + 1);
		}

		@Override
		public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
			// 将数据从列表状态变量中取出，放入 ArrayList，方便排序
			ArrayList<UrlViewCount> urlViewCountArrayList = new ArrayList<>();
			for (UrlViewCount urlViewCount : urlViewCountListState.get()) {
				urlViewCountArrayList.add(urlViewCount);
			}
			// 清空状态，释放资源
			urlViewCountListState.clear();
			// 排序
			urlViewCountArrayList.sort(new Comparator<UrlViewCount>() {
				@Override
				public int compare(UrlViewCount o1, UrlViewCount o2) {
					return o2.count.intValue() - o1.count.intValue();
				}
			});
			// 取前两名，构建输出结果
			StringBuilder result = new StringBuilder();
			result.append("========================================\n");
			result.append("窗口结束时间：" + new Timestamp(timestamp - 1) + "\n");
			for (int i = 0; i < this.n; i++) {
				UrlViewCount UrlViewCount = urlViewCountArrayList.get(i);
				String info = "No." + (i + 1) + " "
						+ "url：" + UrlViewCount.url + " "
						+ "浏览量：" + UrlViewCount.count + "\n";
				result.append(info);
			}
			result.append("========================================\n");
			out.collect(result.toString());
		}
	}
}
