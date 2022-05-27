package com.bingoabin._07processfunction;

/**
 * @author bingoabin
 * @date 2022/5/22 22:08
 */

import com.bingoabin._02source.ClickSource;
import com.bingoabin.pojo.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

//我们之前学习的转换算子，一般只是针对某种具体操作来定义的，能够拿到的信息比较有
// 限。比如 map 算子，我们实现的 MapFunction 中，只能获取到当前的数据，定义它转换之后
// 的形式；而像窗口聚合这样的复杂操作，AggregateFunction 中除数据外，还可以获取到当前的
// 状态（以累加器 Accumulator 形式出现）。另外我们还介绍过富函数类，比如 RichMapFunction，
// 它提供了获取运行时上下文的方法 getRuntimeContext()，可以拿到状态，还有并行度、任务名
// 称之类的运行时信息。
// 但是无论那种算子，如果我们想要访问事件的时间戳，或者当前的水位线信息，都是完全
// 做不到的。在定义生成规则之后，水位线会源源不断地产生，像数据一样在任务间流动，可我
// 们却不能像数据一样去处理它；跟时间相关的操作，目前我们只会用窗口来处理。而在很多应
// 用需求中，要求我们对时间有更精细的控制，需要能够获取水位线，甚至要“把控时间”、定义
// 什么时候做什么事，这就不是基本的时间窗口能够实现的了。
// 于是必须祭出大招——处理函数（ProcessFunction）了。处理函数提供了一个“定时服务”
// （TimerService），我们可以通过它访问流中的事件（event）、时间戳（timestamp）、水位线
// （watermark），甚至可以注册“定时事件”。而且处理函数继承了 AbstractRichFunction 抽象类，
// 所以拥有富函数类的所有特性，同样可以访问状态（state）和其他运行时信息。此外，处理函
// 数还可以直接将数据输出到侧输出流（side output）中。所以，处理函数是最为灵活的处理方
// 法，可以实现各种自定义的业务逻辑；同时也是整个 DataStream API 的底层基础。
public class ProcessFunctionTest {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.addSource(new ClickSource())
		   .assignTimestampsAndWatermarks(
				   WatermarkStrategy.<Event>forMonotonousTimestamps()
				                    .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
					                    @Override
					                    public long extractTimestamp(Event event, long l) {
						                    return event.timestamp;
					                    }
				                    })
		                                 )
		   .process(new ProcessFunction<Event, String>() {
			   @Override
			   public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
				   if (value.user.equals("Mary")) {
					   out.collect(value.user);
				   } else if (value.user.equals("Bob")) {
					   out.collect(value.user);
					   out.collect(value.user);
				   }
				   System.out.println(ctx.timerService().currentWatermark());
			   }
		   })
		   .print();
		env.execute();
	}
}
