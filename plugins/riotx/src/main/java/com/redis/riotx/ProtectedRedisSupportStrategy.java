package com.redis.riotx;

import org.springframework.util.StringUtils;

import com.redis.spring.batch.item.redis.common.RedisInfo;
import com.redis.spring.batch.item.redis.common.RedisSupport;
import com.redis.spring.batch.item.redis.common.RedisSupportStatus;
import com.redis.spring.batch.item.redis.common.RedisSupportStrategy;

public class ProtectedRedisSupportStrategy implements RedisSupportStrategy {

	@Override
	public void check(RedisSupport support) {
		if (support.getStatus() == RedisSupportStatus.UNSUPPORTED) {
			throw new UnsupportedOperationException(message(support.getInfo()));
		}
	}

	private String message(RedisInfo info) {
		if (StringUtils.hasLength(info.getOS())) {
			return info.getOS();
		}
		return info.getServerName();
	}

}
