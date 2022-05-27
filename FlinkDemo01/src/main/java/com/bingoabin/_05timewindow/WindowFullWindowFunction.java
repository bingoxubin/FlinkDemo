package com.bingoabin._05timewindow;

/**
 * @author bingoabin
 * @date 2022/5/22 21:16
 */
public class WindowFullWindowFunction {
	//窗口操作中的另一大类就是全窗口函数。与增量聚合函数不同，全窗口函数需要先收集窗
	// 口中的数据，并在内部缓存起来，等到窗口要输出结果的时候再取出数据进行计算。
	// 很明显，这就是典型的批处理思路了——先攒数据，等一批都到齐了再正式启动处理流程。
	// 这样做毫无疑问是低效的：因为窗口全部的计算任务都积压在了要输出结果的那一瞬间，而在
	// 之前收集数据的漫长过程中却无所事事。这就好比平时不用功，到考试之前通宵抱佛脚，肯定
	// 不如把工夫花在日常积累上。
	// 那为什么还需要有全窗口函数呢？这是因为有些场景下，我们要做的计算必须基于全部的
	// 数据才有效，这时做增量聚合就没什么意义了；另外，输出的结果有可能要包含上下文中的一
	// 些信息（比如窗口的起始时间），这是增量聚合函数做不到的。所以，我们还需要有更丰富的
	// 窗口计算方式，这就可以用全窗口函数来实现。

	//全窗口函数（full window functions） 全窗口函数也有两种：WindowFunction 和 ProcessWindowFunction。
	//窗口函数（WindowFunction）
	public static void main(String[] args){
	    //stream
		//  .keyBy(<key selector>)
		//  .window(<window assigner>)
		//  .apply(new MyWindowFunction());
	}
}
