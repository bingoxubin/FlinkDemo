package com.bingoabin._03transformation._04physicalpatition;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author bingoabin
 * @date 2022/5/22 11:38
 */
public class CustomTest {
	//自定义分区（Custom）
	//在调用时，方法需要传入两个参数，第一个是自定义分区器（Partitioner）对象，第二个
	// 是应用分区器的字段，它的指定方式与 keyBy 指定 key 基本一样：可以通过字段名称指定，
	// 也可以通过字段位置索引来指定，还可以实现一个 KeySelector

	//按照奇偶性进行重分区
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		// 将自然数按照奇偶分区
		env.fromElements(1, 2, 3, 4, 5, 6, 7, 8)
		   .partitionCustom(new Partitioner<Integer>() {
			   @Override
			   public int partition(Integer key, int numPartitions) {
				   return key % 2;
			   }
		   }, new KeySelector<Integer, Integer>() {
			   @Override
			   public Integer getKey(Integer value) throws Exception {
				   return value;
			   }
		   })
		   .print().setParallelism(2);
		env.execute();
	}
}
