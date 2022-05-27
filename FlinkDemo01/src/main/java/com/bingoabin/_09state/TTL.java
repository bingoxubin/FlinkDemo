package com.bingoabin._09state;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;

/**
 * @author bingoabin
 * @date 2022/5/23 10:39
 */
public class TTL {
	public static void main(String[] args) {
		//在实际应用中，很多状态会随着时间的推移逐渐增长，如果不加以限制，最终就会导致存
		// 储空间的耗尽。一个优化的思路是直接在代码中调用.clear()方法去清除状态，但是有时候我们
		// 的逻辑要求不能直接清除。这时就需要配置一个状态的“生存时间”（time-to-live，TTL），当
		// 状态在内存中存在的时间超出这个值时，就将它清除。
		// 具体实现上，如果用一个进程不停地扫描所有状态看是否过期，显然会占用大量资源做无
		// 用功。状态的失效其实不需要立即删除，所以我们可以给状态附加一个属性，也就是状态的“失
		// 效时间”。状态创建的时候，设置 失效时间 = 当前时间 + TTL；之后如果有对状态的访问和
		// 修改，我们可以再对失效时间进行更新；当设置的清除条件被触发时（比如，状态被访问的时
		// 候，或者每隔一段时间扫描一次失效状态），就可以判断状态是否失效、从而进行清除了。
		// 配置状态的 TTL 时，需要创建一个 StateTtlConfig 配置对象，然后调用状态描述器
		// 的.enableTimeToLive()方法启动 TTL 功能。

		StateTtlConfig ttlConfig = StateTtlConfig
				.newBuilder(Time.seconds(10))
				.setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
				.setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
				.build();
		ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("my state", String.class);
		stateDescriptor.enableTimeToLive(ttlConfig);
	}
}
