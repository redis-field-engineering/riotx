package com.redis.riotx;

public class Offset {

	private String offset;

	public String getOffset() {
		return offset;
	}

	public void setOffset(String offset) {
		this.offset = offset;
	}

	public static Offset create(String string) {
		Offset offset = new Offset();
		offset.setOffset(string);
		return offset;
	}

}
