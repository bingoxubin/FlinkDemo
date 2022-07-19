package com.bingoabin;

import com.streamxhub.streamx.flink.core.StreamEnvConfig;
import com.streamxhub.streamx.flink.core.java.source.KafkaSource;
import com.streamxhub.streamx.flink.core.scala.StreamingContext;
import com.streamxhub.streamx.flink.core.scala.source.KafkaRecord;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @author bingoabin
 * @date 2022/7/19 15:26
 */
public class FlinkTest {
	public static void main(String[] args) {
		// 配置
		StreamEnvConfig javaConfig = new StreamEnvConfig(args, null);
		// 创建 StreamingContext 对象, 是一个核心类
		StreamingContext ctx = new StreamingContext(javaConfig);
		// 消费 kafka 数据
		new KafkaSource<String>(ctx).getDataStream().map(new MapFunction<KafkaRecord<String>, String>() {
			@Override
			public String map(KafkaRecord<String> value) throws Exception {
				return value.value();
			}
		}).print();
		// 启动任务
		ctx.start();
	}
}
