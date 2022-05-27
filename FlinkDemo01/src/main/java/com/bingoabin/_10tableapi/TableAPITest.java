package com.bingoabin._10tableapi;

import com.bingoabin.pojo.Event;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author bingoabin
 * @date 2022/5/23 12:03
 */
public class TableAPITest {
	public static void main(String[] args) throws Exception {
		// 获取流执行环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		// 读取数据源
		SingleOutputStreamOperator<Event> eventStream = env
				.fromElements(
						new Event("Alice", "./home", 1000L),
						new Event("Bob", "./cart", 1000L),
						new Event("Alice", "./prod?id=1", 5 * 1000L),
						new Event("Cary", "./home", 60 * 1000L),
						new Event("Bob", "./prod?id=3", 90 * 1000L),
						new Event("Alice", "./prod?id=7", 105 * 1000L)
				             );
		// 获取表环境
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
		// 将数据流转换成表
		Table eventTable = tableEnv.fromDataStream(eventStream);
		// 用执行 SQL 的方式提取数据
		Table visitTable = tableEnv.sqlQuery("select url, user from " + eventTable);
		// 将表转换成数据流，打印输出
		tableEnv.toDataStream(visitTable).print();
		// 执行程序
		env.execute();
	}

	//// 创建表环境
	// TableEnvironment tableEnv = ...;
	// // 创建输入表，连接外部系统读取数据
	// tableEnv.executeSql("CREATE TEMPORARY TABLE inputTable ... WITH ( 'connector'
	// = ... )");
	// // 注册一个表，连接到外部系统，用于输出
	// tableEnv.executeSql("CREATE TEMPORARY TABLE outputTable ... WITH ( 'connector'
	// = ... )");
	// // 执行 SQL 对表进行查询转换，得到一个新的表
	// Table table1 = tableEnv.sqlQuery("SELECT ... FROM inputTable... ");
	// // 使用 Table API 对表进行查询转换，得到一个新的表
	// Table table2 = tableEnv.from("inputTable").select(...);
	// // 将得到的结果写入输出表
	// TableResult tableResult = table1.executeInsert("outputTable");
}
