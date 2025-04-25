package com.redis.spring.batch.item.redis.common;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.util.StringUtils;

import io.lettuce.core.AbstractRedisClient;

public class RedisSupportCheck implements Consumer<AbstractRedisClient> {

	private static final String UNSUPPORTED_OS = "(?i).*elasticache.*";
	private static final String UNSUPPORTED_SERVER = "(?i).*valkey.*";

	private final Log log = LogFactory.getLog(getClass());
	private final List<Consumer<RedisInfo>> consumers = new ArrayList<>(Arrays.asList(this::logUnsupported));

	@Override
	public void accept(AbstractRedisClient client) {
		RedisInfo info = RedisInfo.from(client);
		if (isUnsupported(info.getServerName(), UNSUPPORTED_SERVER) || isUnsupported(info.getOS(), UNSUPPORTED_OS)) {
			consumers.forEach(c -> c.accept(info));
		}
	}

	public List<Consumer<RedisInfo>> getConsumers() {
		return consumers;
	}

	private void logUnsupported(RedisInfo info) {
		log.warn(Messages.getString("RedisSupportCheck.message"));
	}

	private boolean isUnsupported(String property, String regex) {
		return StringUtils.hasLength(property) && property.matches(regex);
	}

}
