package redis.clients.jedis.tests.unix;

import java.io.IOException;

import junit.framework.Assert;

import org.junit.Test;

import redis.clients.jedis.Jedis;

public class UnixDomainJedisTest {

	@Test
	public void testUnixDomainSocket() throws IOException{
		Jedis connection = new Jedis("/var/run/redis/redis1.sock", true);
		connection.auth("foobared");
		connection.flushAll();
		connection.set("UNIX", "HI!");
		Assert.assertEquals("HI!", connection.get("UNIX"));
	}
	
}
