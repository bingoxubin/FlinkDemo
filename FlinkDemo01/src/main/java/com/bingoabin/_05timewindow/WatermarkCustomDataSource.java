package com.bingoabin._05timewindow;

import com.bingoabin.pojo.Event;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.Calendar;
import java.util.Random;

/**
 * @author bingoabin
 * @date 2022/5/22 18:41
 */
public class WatermarkCustomDataSource {
	//在自定义数据源中发送水位线
	//我们也可以在自定义的数据源中抽取事件时间，然后发送水位线。这里要注意的是，在自
	// 定义数据源中发送了水位线以后，就不能再在程序中使用 assignTimestampsAndWatermarks 方
	// 法 来 生 成 水 位 线 了 。 在 自 定 义 数 据 源 中 生 成 水 位 线 和 在 程 序 中 使 用
	// assignTimestampsAndWatermarks 方法生成水位线二者只能取其一。
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.addSource(new ClickSourceWithWatermark()).print();
		env.execute();
	}

	// 泛型是数据源中的类型
	public static class ClickSourceWithWatermark implements SourceFunction<Event> {
		private boolean running = true;
		@Override
		public void run(SourceFunction.SourceContext<Event> sourceContext) throws Exception {
			Random random = new Random();
			String[] userArr = {"Mary", "Bob", "Alice"};
			String[] urlArr = {"./home", "./cart", "./prod?id=1"};
			while (running) {
				long currTs = Calendar.getInstance().getTimeInMillis(); // 毫秒时间戳
				String username = userArr[random.nextInt(userArr.length)];
				String url = urlArr[random.nextInt(urlArr.length)];
				Event event = new Event(username, url, currTs);
				// 使用 collectWithTimestamp 方法将数据发送出去，并指明数据中的时间戳的字段
				sourceContext.collectWithTimestamp(event, event.timestamp);
				// 发送水位线
				sourceContext.emitWatermark(new Watermark(event.timestamp - 1L));
				Thread.sleep(1000L);
			}
		}

		@Override
		public void cancel() {
			running = false;
		}
	}
}
