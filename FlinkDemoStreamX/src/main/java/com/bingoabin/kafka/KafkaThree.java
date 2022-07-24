package com.bingoabin.kafka;

import com.streamxhub.streamx.flink.core.StreamEnvConfig;
import com.streamxhub.streamx.flink.core.java.source.KafkaSource;
import com.streamxhub.streamx.flink.core.scala.StreamingContext;

/**
 * @author bingoabin
 * @date 2022/7/21 19:16
 */
public class KafkaThree {
	public static void main(String[] args) {
		//Flink API 写法
		// StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		//StreamX API 写法
		// 配置
		StreamEnvConfig javaConfig = new StreamEnvConfig(args, null);
		// 创建 StreamingContext 对象, 是一个核心类
		StreamingContext ctx = new StreamingContext(javaConfig);
		// 消费 kafka 数据
		new KafkaSource<String>(ctx).alias("kafka1").topic("bingo").getDataStream().map(record -> record.value()).print("bingo");
		new KafkaSource<String>(ctx).alias("kafka2").topic("bingo1").getDataStream().map(record -> record.value()).print("bingo1");
		// 启动任务
		ctx.start();
	}

	//创建topic
	//kafka-topics.sh --create --partitions 3 --replication-factor 2 --topic bingo --zookeeper node01:2181,node02:2181,node03:2181
	//kafka-topics.sh --create --partitions 3 --replication-factor 2 --topic bingo1 --zookeeper node01:2181,node02:2181,node03:2181
	//生产消息
	//kafka-console-producer.sh --broker-list node01:9092,node02:9092,node03:9092 --topic bingo
	//kafka-console-producer.sh --broker-list node01:9092,node02:9092,node03:9092 --topic bingo1

	//--conf F:\project\FlinkDemo\FlinkDemoStreamX\assembly\conf\application.yml
}
