package com.redis.spring.batch.item.redis.gen;

import com.redis.spring.batch.item.redis.common.Range;

public class CollectionOptions {

	public static final Range DEFAULT_MEMBER_RANGE = new Range(1, 100);
	public static final Range DEFAULT_MEMBER_COUNT = new Range(100, 100);

	private Range memberRange = DEFAULT_MEMBER_RANGE;

	private Range memberCount = DEFAULT_MEMBER_COUNT;

	public Range getMemberRange() {
		return memberRange;
	}

	public void setMemberRange(Range range) {
		this.memberRange = range;
	}

	public Range getMemberCount() {
		return memberCount;
	}

	public void setMemberCount(Range count) {
		this.memberCount = count;
	}

}
