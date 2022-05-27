package com.bingoabin._05timewindow;

import com.bingoabin._02source.ClickSource;
import com.bingoabin.pojo.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;

/**
 * @author bingoabin
 * @date 2022/5/22 21:16
 */
public class WindowFullProcessWindowFunction {
	//窗口操作中的另一大类就是全窗口函数。与增量聚合函数不同，全窗口函数需要先收集窗
	// 口中的数据，并在内部缓存起来，等到窗口要输出结果的时候再取出数据进行计算。
	// 很明显，这就是典型的批处理思路了——先攒数据，等一批都到齐了再正式启动处理流程。
	// 这样做毫无疑问是低效的：因为窗口全部的计算任务都积压在了要输出结果的那一瞬间，而在
	// 之前收集数据的漫长过程中却无所事事。这就好比平时不用功，到考试之前通宵抱佛脚，肯定
	// 不如把工夫花在日常积累上。
	// 那为什么还需要有全窗口函数呢？这是因为有些场景下，我们要做的计算必须基于全部的
	// 数据才有效，这时做增量聚合就没什么意义了；另外，输出的结果有可能要包含上下文中的一
	// 些信息（比如窗口的起始时间），这是增量聚合函数做不到的。所以，我们还需要有更丰富的
	// 窗口计算方式，这就可以用全窗口函数来实现。

	//全窗口函数（full window functions） 全窗口函数也有两种：WindowFunction 和 ProcessWindowFunction。
	//处理窗口函数（ProcessWindowFunction）
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		SingleOutputStreamOperator<Event> stream = env
				.addSource(new ClickSource())
				.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
				                                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
					                                                @Override
					                                                public long extractTimestamp(Event element, long
							                                                recordTimestamp) {
						                                                return element.timestamp;
					                                                }
				                                                }));
		// 将数据全部发往同一分区，按窗口统计 UV
		stream.keyBy(data -> true)
		      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
		      .process(new UvCountByWindow())
		      .print();
		env.execute();
	}

	// 自定义窗口处理函数
	public static class UvCountByWindow extends ProcessWindowFunction<Event, String, Boolean, TimeWindow> {
		@Override
		public void process(Boolean aBoolean, Context context, Iterable<Event> elements, Collector<String> out) throws Exception {
			HashSet<String> userSet = new HashSet<>();
			// 遍历所有数据，放到 Set 里去重
			for (Event event : elements) {
				userSet.add(event.user);
			}
			// 结合窗口信息，包装输出内容
			Long start = context.window().getStart();
			Long end = context.window().getEnd();
			out.collect(" 窗 口 : " + new Timestamp(start) + " ~ " + new Timestamp(end) + " 的独立访客数量是：" + userSet.size());
		}
	}
}
