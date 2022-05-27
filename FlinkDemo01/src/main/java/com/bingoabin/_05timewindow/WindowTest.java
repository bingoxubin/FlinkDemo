package com.bingoabin._05timewindow;

/**
 * @author bingoabin
 * @date 2022/5/22 20:20
 */
public class WindowTest {
	public static void main(String[] args){
	    //滚动处理时间窗口
		//stream.keyBy(...)
		// .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
		// .aggregate(...)
		//如果窗口是一天，默认用伦敦时间开始，需要用北京时间的话，可以加第二个参数，偏移量
		//.window(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)))

		//滑动处理时间窗口
		//stream.keyBy(...)
		// .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
		// .aggregate(...)

		//处理时间会话窗口
		//stream.keyBy(...)
		// .window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))
		// .aggregate(...)

		//滚动事件时间窗口
		//stream.keyBy(...)
		// .window(TumblingEventTimeWindows.of(Time.seconds(5)))
		// .aggregate(...)

		//滑动事件时间窗口
		//stream.keyBy(...)
		// .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
		// .aggregate(...)

		//事件时间会话窗口
		//stream.keyBy(...)
		// .window(EventTimeSessionWindows.withGap(Time.seconds(10)))
		// .aggregate(...)

		//滚动计数窗口
		//stream.keyBy(...)
		// .countWindow(10)

		//滑动计数窗口
		//stream.keyBy(...)
		// .countWindow(10，3)

		//全局窗口
		//stream.keyBy(...)
		// .window(GlobalWindows.create());
	}
}
