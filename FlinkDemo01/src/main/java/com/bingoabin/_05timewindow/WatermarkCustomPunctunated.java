package com.bingoabin._05timewindow;

import com.bingoabin._02source.ClickSource;
import com.bingoabin.pojo.Event;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author bingoabin
 * @date 2022/5/22 18:39
 */
public class WatermarkCustomPunctunated {
	//两种不同的生成水位线的方式：一种是周期性的（Periodic），另一种是断点式的（Punctuated）。
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.addSource(new ClickSource())
		   .assignTimestampsAndWatermarks(new WatermarkCustomPeriodic.CustomWatermarkStrategy())
		   .print();
		env.execute();
	}

	public class CustomPunctuatedGenerator implements WatermarkGenerator<Event> {
		@Override
		public void onEvent(Event r, long eventTimestamp, WatermarkOutput output) {
			// 只有在遇到特定的 itemId 时，才发出水位线
			if (r.user.equals("Mary")) {
				output.emitWatermark(new Watermark(r.timestamp - 1));
			}
		}

		@Override
		public void onPeriodicEmit(WatermarkOutput output) {
			// 不需要做任何事情，因为我们在 onEvent 方法中发射了水位线
		}
	}
}
