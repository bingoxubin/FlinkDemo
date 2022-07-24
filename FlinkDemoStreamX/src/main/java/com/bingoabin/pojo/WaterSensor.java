package com.bingoabin.pojo;

/**
 * @author bingoabin
 * @date 2022/7/21 19:23
 */
public class WaterSensor {
	private String id;
	private Long ts;
	private Integer vc;

	public WaterSensor() {
	}

	public WaterSensor(String id, Long ts, Integer vc) {
		this.id = id;
		this.ts = ts;
		this.vc = vc;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Long getTs() {
		return ts;
	}

	public void setTs(Long ts) {
		this.ts = ts;
	}

	public Integer getVc() {
		return vc;
	}

	public void setVc(Integer vc) {
		this.vc = vc;
	}

	@Override
	public String toString() {
		return "Event{" +
				"id='" + id + '\'' +
				", ts=" + ts +
				", vc=" + vc +
				'}';
	}
}
