package com.bingoabin._02source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @author bingoabin
 * @date 2022/5/21 22:27
 */
public class ReadFromKafka {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "hadoop:9092");
		properties.setProperty("group.id", "consumer-group");
		properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.setProperty("auto.offset.reset", "latest");

		//第一个参数 topic，定义了从哪些主题中读取数据。可以是一个 topic，也可以是 topic列表，
		// 还可以是匹配所有想要读取的 topic 的正则表达式。当从多个 topic 中读取数据
		// 时，Kafka 连接器将会处理所有 topic 的分区，将这些分区的数据放到一条流中去。
		//第二个参数是一个 DeserializationSchema 或者 KeyedDeserializationSchema。Kafka 消
		// 息被存储为原始的字节数据，所以需要反序列化成 Java 或者 Scala 对象。上面代码中
		// 使用的 SimpleStringSchema，是一个内置的 DeserializationSchema，它只是将字节数
		// 组简单地反序列化成字符串。DeserializationSchema 和 KeyedDeserializationSchema 是
		// 公共接口，所以我们也可以自定义反序列化逻辑。
		//第三个参数是一个 Properties 对象，设置了 Kafka 客户端的一些属性。
		DataStreamSource<String> stream = env.addSource(new FlinkKafkaConsumer<String>("clicks", new SimpleStringSchema(), properties));
		stream.print("kafka");
		env.execute();
	}
}
