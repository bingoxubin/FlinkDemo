package com.bingoabin.sql;

import com.streamxhub.streamx.flink.core.TableContext;
import com.streamxhub.streamx.flink.core.TableEnvConfig;

/**
 * @author bingoabin
 * @date 2022/7/22 9:48
 */
public class StreamXSqlDemo {
	public static void main(String[] args){
		TableEnvConfig tableEnvConfig = new TableEnvConfig(args, null);
		TableContext tableContext = new TableContext(tableEnvConfig);

		//flink sql写法
		// tableContext.executeSql("");

		//stream sql写法
		tableContext.sql("first");

		//--conf F:\project\FlinkDemo\FlinkDemoStreamX\assembly\conf\application.yml --sql F:\project\FlinkDemo\FlinkDemoStreamX\assembly\conf\sql.yml
	}
}
