package redis.clients.jedis;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;

import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.util.RedisInputStream;
import redis.clients.util.RedisOutputStream;

public class TcpConnection extends Connection implements Closeable {

	protected String host;
	protected int port = Protocol.DEFAULT_PORT;
	
	public String getHost() {
	return host;
	}

	public void setHost(final String host) {
	this.host = host;
	}

	public int getPort() {
	return port;
	}

	public void setPort(final int port) {
	this.port = port;
	}

	public void setTimeout(final int timeout) {
	this.timeout = timeout;
	}

	public int getTimeout() {
	return timeout;
	}

	public void setTimeoutInfinite() {
	try {
	    if (!isConnected()) {
		connect();
	    }
	    socket.setKeepAlive(true);
	    socket.setSoTimeout(0);
	} catch (SocketException ex) {
	    throw new JedisException(ex);
	}
	}

	public void rollbackTimeout() {
	try {
	    socket.setSoTimeout(timeout);
	    socket.setKeepAlive(false);
	} catch (SocketException ex) {
	    throw new JedisException(ex);
	}
	}

	
    public TcpConnection(final String host) {
	super();
	this.host = host;
    }

    public TcpConnection(final String host, final int port) {
	super();
	this.host = host;
	this.port = port;
    }

    public TcpConnection() {
    }
    

	public void connect() {
	if (!isConnected()) {
	    try {
		socket = new Socket();
		// ->@wjw_add
		socket.setReuseAddress(true);
		socket.setKeepAlive(true); // Will monitor the TCP connection is
					   // valid
		socket.setTcpNoDelay(true); // Socket buffer Whetherclosed, to
					    // ensure timely delivery of data
		socket.setSoLinger(true, 0); // Control calls close () method,
					     // the underlying socket is closed
					     // immediately
		// <-@wjw_add
	
		socket.connect(new InetSocketAddress(host, port), timeout);
		socket.setSoTimeout(timeout);
		outputStream = new RedisOutputStream(socket.getOutputStream());
		inputStream = new RedisInputStream(socket.getInputStream());
	    } catch (IOException ex) {
		throw new JedisConnectionException(ex);
	    }
	}
	}
}
