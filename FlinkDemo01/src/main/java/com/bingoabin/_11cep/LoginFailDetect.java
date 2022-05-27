package com.bingoabin._11cep;

/**
 * @author bingoabin
 * @date 2022/5/23 12:28
 */

import com.bingoabin.pojo.LoginEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;
import java.util.Map;
public class LoginFailDetect {
	//检测用户行为，如果连续三次登录失败，就输出报警信息。

	//接下来就是业务逻辑的编写。Flink CEP 在代码中主要通过 Pattern API 来实现。之前我们
	// 已经介绍过，CEP 的主要处理流程分为三步，对应到 Pattern API 中就是：
	// （1）定义一个模式（Pattern）；
	// （2）将Pattern应用到DataStream上，检测满足规则的复杂事件，得到一个PatternStream；
	// （3）对 PatternStream 进行转换处理，将检测到的复杂事件提取出来，包装成报警信息输出。
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		// 获取登录事件流，并提取时间戳、生成水位线
		KeyedStream<LoginEvent, String> stream = env
				.fromElements(
						new LoginEvent("user_1", "192.168.0.1", "fail", 2000L),
						new LoginEvent("user_1", "192.168.0.2", "fail", 3000L),
						new LoginEvent("user_2", "192.168.1.29", "fail", 4000L),
						new LoginEvent("user_1", "171.56.23.10", "fail", 5000L),
						new LoginEvent("user_2", "192.168.1.29", "success", 6000L),
						new LoginEvent("user_2", "192.168.1.29", "fail", 7000L),
						new LoginEvent("user_2", "192.168.1.29", "fail", 8000L))
				.assignTimestampsAndWatermarks(
						WatermarkStrategy.<LoginEvent>forMonotonousTimestamps()
						                 .withTimestampAssigner(
								                 new SerializableTimestampAssigner<LoginEvent>() {
									                 @Override
									                 public long extractTimestamp(LoginEvent loginEvent, long l) {
										                 return loginEvent.timestamp;
									                 }
								                 }))
				.keyBy(r -> r.userId);
		// 1. 定义 Pattern，连续的三个登录失败事件
		Pattern<LoginEvent, LoginEvent> pattern = Pattern
				.<LoginEvent>begin("first") // 以第一个登录失败事件开始
				.where(new SimpleCondition<LoginEvent>() {
					@Override
					public boolean filter(LoginEvent loginEvent) throws Exception {
						return loginEvent.eventType.equals("fail");
					}
				})
				.next("second") // 接着是第二个登录失败事件
				.where(new SimpleCondition<LoginEvent>() {
					@Override
					public boolean filter(LoginEvent loginEvent) throws Exception {
						return loginEvent.eventType.equals("fail");
					}
				})
				.next("third") // 接着是第三个登录失败事件
				.where(new SimpleCondition<LoginEvent>() {
					@Override
					public boolean filter(LoginEvent loginEvent) throws Exception {
						return loginEvent.eventType.equals("fail");
					}
				});
		// 2. 将 Pattern 应用到流上，检测匹配的复杂事件，得到一个 PatternStream
		PatternStream<LoginEvent> patternStream = CEP.pattern(stream, pattern);
		// 3. 将匹配到的复杂事件选择出来，然后包装成字符串报警信息输出
		patternStream
				.select(new PatternSelectFunction<LoginEvent, String>() {
					@Override
					public String select(Map<String, List<LoginEvent>> map) throws Exception {
						LoginEvent first = map.get("first").get(0);
						LoginEvent second = map.get("second").get(0);
						LoginEvent third = map.get("third").get(0);
						return first.userId + " 连续三次登录失败！登录时间：" +
								first.timestamp + ", " + second.timestamp + ", " + third.timestamp;
					}
				})
				.print("warning");
		env.execute();
	}
}
