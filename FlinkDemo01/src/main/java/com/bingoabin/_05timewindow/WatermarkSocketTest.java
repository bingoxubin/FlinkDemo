package com.bingoabin._05timewindow;

import com.bingoabin.pojo.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author bingoabin
 * @date 2022/5/22 21:37
 */
public class WatermarkSocketTest {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		// 将数据源改为 socket 文本流，并转换成 Event 类型
		env.socketTextStream("localhost", 7777)
		   .map(new MapFunction<String, Event>() {
			   @Override
			   public Event map(String value) throws Exception {
				   String[] fields = value.split(",");
				   return new Event(fields[0].trim(), fields[1].trim(),
				                    Long.valueOf(fields[2].trim()));
			   }
		   })
		   // 插入水位线的逻辑
		   .assignTimestampsAndWatermarks(
				   // 针对乱序流插入水位线，延迟时间设置为 5s
				   WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
				                    .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
					                    // 抽取时间戳的逻辑
					                    @Override
					                    public long extractTimestamp(Event element, long
							                    recordTimestamp) {
						                    return element.timestamp;
					                    }
				                    })
		                                 )
		   // 根据 user 分组，开窗统计
		   .keyBy(data -> data.user)
		   .window(TumblingEventTimeWindows.of(Time.seconds(10)))
		   .process(new WatermarkTestResult())
		   .print();
		env.execute();
	}

	// 自定义处理窗口函数，输出当前的水位线和窗口信息
	public static class WatermarkTestResult extends ProcessWindowFunction<Event, String, String, TimeWindow> {
		@Override
		public void process(String s, Context context, Iterable<Event> elements, Collector<String> out) throws Exception {
			Long start = context.window().getStart();
			Long end = context.window().getEnd();
			Long currentWatermark = context.currentWatermark();
			Long count = elements.spliterator().getExactSizeIfKnown();
			out.collect("窗口" + start + " ~ " + end + "中共有" + count + "个元素， 窗口闭合计算时，水位线处于：" + currentWatermark);
		}
	}

	//我们这里设置的最大延迟时间是 5 秒，所以当我们在终端启动 nc 程序，也就是 nc –lk 7777
	// 然后输入如下数据时：
	// Alice, ./home, 1000
	// Alice, ./cart, 2000
	// Alice, ./prod?id=100, 10000
	// Alice, ./prod?id=200, 8000
	// Alice, ./prod?id=300, 15000
	// 我们会看到如下结果：
	// 窗口 0 ~ 10000 中共有 3 个元素，窗口闭合计算时，水位线处于：9999
	// 我们就会发现，当最后输入[Alice, ./prod?id=300, 15000]时，流中会周期性地（默认 200
	// 毫秒）插入一个时间戳为 15000L – 5 * 1000L – 1L = 9999 毫秒的水位线，已经到达了窗口
	// [0,10000)的结束时间，所以会触发窗口的闭合计算。而后面再输入一条[Alice, ./prod?id=200,
	// 9000]时，将不会有任何结果；因为这是一条迟到数据，它所属于的窗口已经触发计算然后销
	// 毁了（窗口默认被销毁），所以无法再进入到窗口中，自然也就无法更新计算结果了。窗口中
	// 的迟到数据默认会被丢弃，这会导致计算结果不够准确。
}
