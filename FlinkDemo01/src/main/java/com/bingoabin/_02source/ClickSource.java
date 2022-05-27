package com.bingoabin._02source;

import com.bingoabin.pojo.Event;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

/**
 * @author bingoabin
 * @date 2022/5/21 22:44
 */
public class ClickSource implements SourceFunction<Event> {
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
