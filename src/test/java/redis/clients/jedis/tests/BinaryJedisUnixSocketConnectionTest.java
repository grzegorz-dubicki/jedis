package redis.clients.jedis.tests;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.BinaryJedis;
import redis.clients.jedis.Connection;
import redis.clients.jedis.TcpConnection;
import redis.clients.jedis.UnixSocketConnection;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;

public class BinaryJedisUnixSocketConnectionTest extends Assert {
    private BinaryJedis bJedis;

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test(expected = JedisException.class)
    public void checkIncorrectScheme() {
	bJedis = new BinaryJedis("unics://whatever");
	bJedis.connect();
    }

    @Test(expected = JedisException.class)
    public void checkCorrectSchemeWrongFile() {
	bJedis = new BinaryJedis("unix:///this/will/NOT/exist");
	bJedis.connect();
	bJedis.disconnect();
    }
    
    @Test
    public void checkCorrectSchemeCorrectFile() {
	bJedis = new BinaryJedis("unix:///tmp/redis.sock");
	bJedis.connect();
	bJedis.disconnect();
    }
}