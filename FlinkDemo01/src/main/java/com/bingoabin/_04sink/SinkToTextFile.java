package com.bingoabin._04sink;

import com.bingoabin.pojo.Event;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

/**
 * @author bingoabin
 * @date 2022/5/22 11:46
 */
public class SinkToTextFile {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(4);
		DataStreamSource<Event> stream = env.fromElements(new Event("Mary", "./home", 1000L),
		                                                  new Event("Bob", "./cart", 2000L),
		                                                  new Event("Alice", "./prod?id=100", 3000L),
		                                                  new Event("Alice", "./prod?id=200", 3500L),
		                                                  new Event("Bob", "./prod?id=2", 2500L),
		                                                  new Event("Alice", "./prod?id=300", 3600L),
		                                                  new Event("Bob", "./home", 3000L),
		                                                  new Event("Bob", "./prod?id=1", 2300L),
		                                                  new Event("Bob", "./prod?id=3", 3300L));
		StreamingFileSink<String> fileSink = StreamingFileSink
				.<String>forRowFormat(new Path("FlinkDemo01/output"),
				                      new SimpleStringEncoder<>("UTF-8"))
				.withRollingPolicy(
						DefaultRollingPolicy.builder()
						                    .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
						                    .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
						                    .withMaxPartSize(1024 * 1024 * 1024)
						                    .build())
				.build();
		//这里我们创建了一个简单的文件 Sink，通过.withRollingPolicy()方法指定了一个“滚动策
		// 略”。“滚动”的概念在日志文件的写入中经常遇到：因为文件会有内容持续不断地写入，所以
		// 我们应该给一个标准，到什么时候就开启新的文件，将之前的内容归档保存。也就是说，上面
		// 的代码设置了在以下 3 种情况下，我们就会滚动分区文件：
		// ⚫ 至少包含 15 分钟的数据
		// ⚫ 最近 5 分钟没有收到新的数据
		// ⚫ 文件大小已达到 1 GB

		// 将 Event 转换成 String 写入文件
		stream.map(Event::toString).addSink(fileSink);
		env.execute();
	}
}
