package com.bingoabin._06elseapi;

/**
 * @author bingoabin
 * @date 2022/5/22 21:50
 */
public class ElseAPI {
	//1. 触发器（Trigger）
	//stream.keyBy(...)
	//  .window(...)
	//  .trigger(new MyTrigger())

	//2. 移除器（Evictor）
	//stream.keyBy(...)
	//  .window(...)
	//  .evictor(new MyEvictor())

	//3. 允许延迟（Allowed Lateness）
	//stream.keyBy(...)
	//  .window(TumblingEventTimeWindows.of(Time.hours(1)))
	//  .allowedLateness(Time.minutes(1))

	//4. 将迟到的数据放入侧输出流
	//DataStream<Event> stream = env.addSource(...);
	// OutputTag<Event> outputTag = new OutputTag<Event>("late") {};
	// stream.keyBy(...)
	//  .window(TumblingEventTimeWindows.of(Time.hours(1)))
	// .sideOutputLateData(outputTag)
	//将迟到数据放入侧输出流之后，还应该可以将它提取出来。基于窗口处理完成之后的
	// DataStream，调用.getSideOutput()方法，传入对应的输出标签，就可以获取到迟到数据所在的
	// 流了。
	//SingleOutputStreamOperator<AggResult> winAggStream = stream.keyBy(...)
	//  .window(TumblingEventTimeWindows.of(Time.hours(1))) .sideOutputLateData(outputTag)
	// .aggregate(new MyAggregateFunction())
	// DataStream<Event> lateStream = winAggStream.getSideOutput(outputTag);
}
