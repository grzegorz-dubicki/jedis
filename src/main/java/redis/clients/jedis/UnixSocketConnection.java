package redis.clients.jedis;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

import org.newsclub.net.unix.AFUNIXSocket;
import org.newsclub.net.unix.AFUNIXSocketAddress;

import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.util.RedisInputStream;
import redis.clients.util.RedisOutputStream;

public class UnixSocketConnection extends Connection implements Closeable {
	
	protected String pathname;
	
    public void setTimeout(final int timeout) {
    // FIXME: not implemented
    }

    public void setTimeoutInfinite() {
	// FIXME: not implemented
    }

    public void rollbackTimeout() {
    // FIXME: not implemented
    }

	public int getTimeout() {
	return Integer.MAX_VALUE; // FIXME: not a real value, obviously
	}

    public UnixSocketConnection(final String pathname) {
	super();
	this.setPathname(pathname);
    }

    public UnixSocketConnection() {

    }

    public void connect() {
	if (!isConnected()) {
	    try {
	    socket = AFUNIXSocket.connectTo(new AFUNIXSocketAddress(new File(getPathname())));
		outputStream = new RedisOutputStream(socket.getOutputStream());
		inputStream = new RedisInputStream(socket.getInputStream());
	    } catch (IOException ex) {
		throw new JedisConnectionException(ex);
	    }
	}
    }

	public String getPathname() {
		return pathname;
	}

	public void setPathname(String pathname) {
		this.pathname = pathname;
	}

	@Override
	public String getHost() {
		// FIXME: this may get very wrong...		
		return "localhost";
	}

	@Override
	public int getPort() {
		// FIXME: this may get very wrong...
		return 0;
	}
}
