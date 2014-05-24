package redis.clients.jedis.tests.commands;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.ComparisonFailure;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.tests.HostAndPortUtil;
import redis.clients.jedis.tests.JedisTestBase;

public abstract class UnixSocketJedisCommandTestBase extends JedisTestBase {

    protected Jedis jedis;

    public UnixSocketJedisCommandTestBase() {
	super();
    }

    @Before
    public void setUp() throws Exception {
	jedis = new Jedis("unix:///tmp/redis.sock");
	jedis.connect();
	//jedis.auth("foobared");
	jedis.flushAll();
    }

    @After
    public void tearDown() {
	jedis.disconnect();
    }

    protected Jedis createJedis() {
	Jedis j = new Jedis("unix:///tmp/redis.sock");
	j.connect();
	//j.auth("foobared");
	j.flushAll();
	return j;
    }

    protected void assertEquals(List<byte[]> expected, List<byte[]> actual) {
	assertEquals(expected.size(), actual.size());
	for (int n = 0; n < expected.size(); n++) {
	    assertArrayEquals(expected.get(n), actual.get(n));
	}
    }

    protected void assertEquals(Set<byte[]> expected, Set<byte[]> actual) {
	assertEquals(expected.size(), actual.size());
	Iterator<byte[]> e = expected.iterator();
	while (e.hasNext()) {
	    byte[] next = e.next();
	    boolean contained = false;
	    for (byte[] element : expected) {
		if (Arrays.equals(next, element)) {
		    contained = true;
		}
	    }
	    if (!contained) {
		throw new ComparisonFailure("element is missing",
			Arrays.toString(next), actual.toString());
	    }
	}
    }

    protected boolean arrayContains(List<byte[]> array, byte[] expected) {
	for (byte[] a : array) {
	    try {
		assertArrayEquals(a, expected);
		return true;
	    } catch (AssertionError e) {

	    }
	}
	return false;
    }

    protected boolean setContains(Set<byte[]> set, byte[] expected) {
	for (byte[] a : set) {
	    try {
		assertArrayEquals(a, expected);
		return true;
	    } catch (AssertionError e) {

	    }
	}
	return false;
    }
}
