package com.bingoabin._05timewindow;

import com.bingoabin._02source.ClickSource;
import com.bingoabin.pojo.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * @author bingoabin
 * @date 2022/5/22 18:23
 */
public class WatermarkTest {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		//有序流 是时间戳单调增长（Monotonously Increasing Timestamps）
		env.addSource(new ClickSource())
		   .assignTimestampsAndWatermarks(WatermarkStrategy
				                                  .<Event>forMonotonousTimestamps()
				                                  .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
					                                  @Override
					                                  public long extractTimestamp(Event event, long l) {
						                                  return event.timestamp;
					                                  }
				                                  }));

		//乱序流
		//乱序流中生成的水位线真正的时间戳，其实是 当前最大时间戳 – 延迟时间 – 1，这里的单位是毫秒
		//如果考虑有序流，也就是
		// 延迟时间为 0 的情况，那么时间戳为 7 秒的数据到来时，之后其实是还有可能继续来 7 秒的数
		// 据的；所以生成的水位线不是 7 秒，而是 6 秒 999 毫秒，7 秒的数据还可以继续来。
		env.addSource(new ClickSource())
		   .assignTimestampsAndWatermarks(WatermarkStrategy
				                                  .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
				                                  .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
					                                  @Override
					                                  public long extractTimestamp(Event event, long l) {
						                                  return event.timestamp;
					                                  }
				                                  })).print();

		env.execute();
	}
}
