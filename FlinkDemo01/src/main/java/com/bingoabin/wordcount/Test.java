package com.bingoabin.wordcount;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author bingoabin
 * @date 2022/5/23 16:24
 */
public class Test {
	public static void main(String[] args) {
		List<String> list = Arrays.asList("wangwu", "zhangsan", "lisi");
		List<String> res = list.stream()
		                       .filter(e -> e.contains("n"))
		                       .filter(e -> e.length() > 2)
		                       .collect(Collectors.toList());
		System.out.println(res);
	}
}
