package com.bingoabin._09state;

/**
 * @author bingoabin
 * @date 2022/5/23 11:02
 */
public class OperatorStateTest {
	//算子状态（Operator State）就是一个算子并行实例上定义的状态，作用范围被限定为当前
	// 算子任务。算子状态跟数据的 key 无关，所以不同 key 的数据只要被分发到同一个并行子任务，
	// 就会访问到同一个 Operator State。
	// 算子状态的实际应用场景不如 Keyed State 多，一般用在 Source 或 Sink 等与外部系统连接
	// 的算子上，或者完全没有 key 定义的场景。比如 Flink 的 Kafka 连接器中，就用到了算子状态。
	// 在我们给 Source 算子设置并行度后，Kafka 消费者的每一个并行实例，都会为对应的主题
	// （topic）分区维护一个偏移量， 作为算子状态保存起来。这在保证 Flink 应用“精确一次”
	// （exactly-once）状态一致性时非常有用。

	//算子状态也支持不同的结构类型，主要有三种：ListState、UnionListState 和 BroadcastState。
}
