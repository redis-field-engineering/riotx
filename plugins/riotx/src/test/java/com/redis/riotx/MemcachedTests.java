package com.redis.riotx;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.slf4j.simple.SimpleLogger;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.util.ClassUtils;
import org.testcontainers.utility.DockerImageName;

import com.redis.riot.core.ProgressStyle;
import com.redis.spring.batch.JobUtils;
import com.redis.spring.batch.memcached.MemcachedEntry;
import com.redis.spring.batch.memcached.MemcachedGeneratorItemReader;
import com.redis.spring.batch.memcached.MemcachedItemReader;
import com.redis.spring.batch.memcached.MemcachedItemWriter;
import com.redis.testcontainers.MemcachedContainer;
import com.redis.testcontainers.MemcachedServer;

import net.spy.memcached.MemcachedClient;

@TestInstance(Lifecycle.PER_CLASS)
public class MemcachedTests {

	static {
		System.setProperty("net.spy.log.LoggerImpl", "net.spy.memcached.compat.log.SLF4JLogger");
		System.setProperty(SimpleLogger.SHOW_DATE_TIME_KEY, "true");
	}

	private static final DockerImageName imageName = MemcachedContainer.DEFAULT_IMAGE_NAME
			.withTag(MemcachedContainer.DEFAULT_TAG);

	private static final MemcachedContainer source = new MemcachedContainer(imageName);
	private static final MemcachedContainer target = new MemcachedContainer(imageName);

	private JobRepository jobRepository;
	private Supplier<MemcachedClient> clientSupplier;
	private MemcachedClient client;
	private Supplier<MemcachedClient> targetClientSupplier;
	private MemcachedClient targetClient;

	public static String name(TestInfo info) {
		StringBuilder displayName = new StringBuilder(info.getDisplayName().replace("(TestInfo)", ""));
		info.getTestClass().ifPresent(c -> displayName.append("-").append(ClassUtils.getShortName(c)));
		return displayName.toString();
	}

	public static <T> List<T> readAll(ItemReader<T> reader) throws Exception {
		List<T> list = new ArrayList<>();
		T element;
		while ((element = reader.read()) != null) {
			list.add(element);
		}
		return list;
	}

	@BeforeAll
	void setup() throws Exception {
		jobRepository = JobUtils.jobRepositoryFactoryBean(ClassUtils.getShortName(getClass())).getObject();
		source.start();
		target.start();
		clientSupplier = () -> client(source.getMemcachedAddresses());
		client = clientSupplier.get();
		targetClientSupplier = () -> client(target.getMemcachedAddresses());
		targetClient = targetClientSupplier.get();
	}

	private static MemcachedClient client(List<InetSocketAddress> addresses) {
		try {
			return new MemcachedClient(addresses);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@AfterAll
	void teardown() {
		if (client != null) {
			client.shutdown();
		}
		if (source != null) {
			source.stop();
		}
		if (targetClient != null) {
			targetClient.shutdown();
		}
		if (target != null) {
			target.stop();
		}
	}

	@BeforeEach
	void flushall() {
		client.flush();
		targetClient.flush();
	}

	private void compare(long startTime, Collection<MemcachedEntry> expected, Collection<MemcachedEntry> actual)
			throws Exception {
		Assertions.assertFalse(expected.isEmpty());
		Assertions.assertFalse(actual.isEmpty());
		Map<String, MemcachedEntry> entryMap = expected.stream()
				.collect(Collectors.toMap(MemcachedEntry::getKey, Function.identity()));
		Assertions.assertEquals(expected.size(), actual.size());
		actual.forEach(e -> {
			MemcachedEntry entry = entryMap.get(e.getKey());
			Assertions.assertArrayEquals(entry.getValue(), e.getValue());
			long expectedTime = startTime + entry.getExpiration();
			long actualTime = e.getExpiration();
			long delta = Math.abs(expectedTime - actualTime);
			Assertions.assertTrue(delta < 30,
					() -> String.format("Delta: %,d (%s <> %s)", delta, expectedTime, actualTime));
		});
	}

	private List<MemcachedEntry> readAll(Supplier<MemcachedClient> clientSupplier) throws Exception {
		MemcachedItemReader reader = new MemcachedItemReader(clientSupplier);
		reader.setJobRepository(jobRepository);
		reader.setName(UUID.randomUUID().toString());
		try {
			reader.open(new ExecutionContext());
			return readAll(reader);
		} finally {
			reader.close();
		}
	}

	private void write(List<MemcachedEntry> entries) throws Exception {
		MemcachedItemWriter writer = new MemcachedItemWriter(clientSupplier);
		try {
			writer.open(new ExecutionContext());
			writer.write(new Chunk<>(entries));
		} finally {
			writer.close();
		}
	}

	private List<MemcachedEntry> entries(int count) throws Exception {
		MemcachedGeneratorItemReader reader = new MemcachedGeneratorItemReader();
		reader.setMaxItemCount(count);
		try {
			reader.open(new ExecutionContext());
			return readAll(reader);
		} finally {
			reader.close();
		}

	}

	@Test
	void replicate(TestInfo info) throws Exception {
		int count = 123;
		write(entries(count));
		MemcachedReplicate replication = new MemcachedReplicate();
		replication.getJobArgs().getProgressArgs().setStyle(ProgressStyle.NONE);
		replication.setJobName(name(info));
		replication.setJobRepository(jobRepository);
		replication.setSourceAddressList(new InetSocketAddressList(inetSocketAddress(source)));
		replication.setTargetAddressList(new InetSocketAddressList(inetSocketAddress(target)));
		replication.call();
		List<MemcachedEntry> sourceEntries = readAll(clientSupplier);
		List<MemcachedEntry> targetEntries = readAll(targetClientSupplier);
		compare(0, sourceEntries, targetEntries);
	}

	private InetSocketAddress inetSocketAddress(MemcachedServer server) {
		return InetSocketAddress.createUnresolved(server.getMemcachedHost(), server.getMemcachedPort());
	}

}
