package com.redis.batch;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.function.Predicate;

import org.junit.jupiter.api.Test;

public class GlobTests {

	@Test
	void include() {
		Predicate<String> foo = BatchUtils.globPredicate("foo");
		assertTrue(foo.test("foo"));
		assertFalse(foo.test("bar"));
		Predicate<String> fooStar = BatchUtils.globPredicate("foo*");
		assertTrue(fooStar.test("foobar"));
		assertFalse(fooStar.test("barfoo"));
	}

	@Test
	void exclude() {
		Predicate<String> foo = BatchUtils.globPredicate("foo").negate();
		assertFalse(foo.test("foo"));
		assertTrue(foo.test("foa"));
		Predicate<String> fooStar = BatchUtils.globPredicate("foo*").negate();
		assertFalse(fooStar.test("foobar"));
		assertTrue(fooStar.test("barfoo"));
	}

	@Test
	void includeAndExclude() {
		Predicate<String> foo1 = BatchUtils.globPredicate("foo1").and(BatchUtils.globPredicate("foo").negate());
		assertFalse(foo1.test("foo"));
		assertFalse(foo1.test("bar"));
		assertTrue(foo1.test("foo1"));
		Predicate<String> foo1Star = BatchUtils.globPredicate("foo").and(BatchUtils.globPredicate("foo1*").negate());
		assertTrue(foo1Star.test("foo"));
		assertFalse(foo1Star.test("bar"));
		assertFalse(foo1Star.test("foo1"));
	}

}
