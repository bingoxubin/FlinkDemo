package com.bingoabin.mysql;

import com.bingoabin.pojo.WaterSensor;
import com.streamxhub.streamx.flink.core.StreamEnvConfig;
import com.streamxhub.streamx.flink.core.java.function.SQLFromFunction;
import com.streamxhub.streamx.flink.core.java.sink.JdbcSink;
import com.streamxhub.streamx.flink.core.java.source.KafkaSource;
import com.streamxhub.streamx.flink.core.scala.StreamingContext;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

/**
 * @author bingoabin
 * @date 2022/7/19 15:26
 */
public class KafkaOneSinkToMysql {
	public static void main(String[] args) {
		//Flink API 写法
		// StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		//StreamX API 写法
		// 配置
		StreamEnvConfig javaConfig = new StreamEnvConfig(args, null);
		// 创建 StreamingContext 对象, 是一个核心类
		StreamingContext ctx = new StreamingContext(javaConfig);
		// 消费 kafka 数据
		SingleOutputStreamOperator<WaterSensor> kafkaDS = new KafkaSource<String>(ctx).getDataStream().map(record -> {
			String[] split = record.value().split(",");
			return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
		});

		new JdbcSink<WaterSensor>(ctx)
				.sql(new SQLFromFunction<WaterSensor>() {
					@Override
					public String from(WaterSensor waterSensor) {
						return String.format(
								"insert into sensor(id,ts,vc) values ('%s',%d,%d)",
								waterSensor.getId(),
								waterSensor.getTs(),
								waterSensor.getVc());
					}
				})
				.sink(kafkaDS);
		// 启动任务
		ctx.start();
	}

	//创建topic
	//kafka-topics.sh --create --partitions 3 --replication-factor 2 --topic bingo --zookeeper node01:2181,node02:2181,node03:2181
	//生产消息
	//kafka-console-producer.sh --broker-list node01:9092,node02:9092,node03:9092 --topic bingo

	//--conf F:\project\FlinkDemo\FlinkDemoStreamX\assembly\conf\application.yml
}
