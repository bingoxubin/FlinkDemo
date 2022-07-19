package com.bingoabin;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Calendar;
import java.util.Random;

/**
 * @author bingoabin
 * @date 2022/5/21 22:56
 */
public class ReadFromCustomParallel {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.addSource(new CustomSource()).setParallelism(2).print();
		env.execute();
	}

	public static class CustomSource implements ParallelSourceFunction<Event> {
		//声明一个布尔变量，作为控制数据生成的标识
		private Boolean running = true;

		@Override
		public void run(SourceContext<Event> ctx) throws Exception {
			Random random = new Random();
			String[] users = {"Mary", "Alice", "Bob", "Cary"};
			String[] urls = {"./home", "./cart", "./fav", "./prod?id=1", "./prod?id=2"};
			while (running) {
				ctx.collect(new Event(
						users[random.nextInt(users.length)],
						urls[random.nextInt(urls.length)],
						Calendar.getInstance().getTimeInMillis()
				));
				//隔一秒生成一个
				Thread.sleep(1000);
			}
		}

		@Override
		public void cancel() {
			running = false;
		}
	}
}
