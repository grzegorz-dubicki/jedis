package redis.clients.jedis.tests;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.Connection;
import redis.clients.jedis.TcpConnection;
import redis.clients.jedis.UnixSocketConnection;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;

public class UnixSocketConnectionTest extends Assert {
    private UnixSocketConnection client;

    @Before
    public void setUp() throws Exception {
	client = new UnixSocketConnection();
    }

    @After
    public void tearDown() throws Exception {
	client.disconnect();
    }

    @Test(expected = JedisException.class)
    public void checkNonExistingFile() {
	client.setPathname("/this/path/should/not/exist");
	client.connect();
    }

    @Test
    public void checkCloseable() {
	client.setPathname("/tmp/redis.sock");
	client.connect();
	client.disconnect();
    }
}