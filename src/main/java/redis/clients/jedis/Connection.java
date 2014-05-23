package redis.clients.jedis;

import java.io.Closeable;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import redis.clients.jedis.Protocol.Command;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.util.RedisInputStream;
import redis.clients.util.RedisOutputStream;
import redis.clients.util.SafeEncoder;

public abstract class Connection implements Closeable, ConnectionInterface {

	protected Socket socket;
	protected RedisOutputStream outputStream;
	protected RedisInputStream inputStream;
	protected int pipelinedCommands = 0;
	protected int timeout = Protocol.DEFAULT_TIMEOUT;

	public Connection() {
		super();
	}

	/* (non-Javadoc)
	 * @see redis.clients.jedis.ConnectionInterface#getSocket()
	 */
	@Override
	public Socket getSocket() {
	return socket;
	}

	/* (non-Javadoc)
	 * @see redis.clients.jedis.ConnectionInterface#getTimeout()
	 */
	@Override
	public abstract int getTimeout();

	public abstract String getHost();
	
	public abstract int getPort();
	
	public void flush() {
	try {
	    outputStream.flush();
	} catch (IOException e) {
	    throw new JedisConnectionException(e);
	}
	}

	@Override
	public Connection sendCommand(final Command cmd, final String... args) {
	final byte[][] bargs = new byte[args.length][];
	for (int i = 0; i < args.length; i++) {
	    bargs[i] = SafeEncoder.encode(args[i]);
	}
	return sendCommand(cmd, bargs);
	}

	@Override
	public Connection sendCommand(final Command cmd, final byte[]... args) {
	connect();
	Protocol.sendCommand(outputStream, cmd, args);
	pipelinedCommands++;
	return this;
	}

	@Override
	public Connection sendCommand(final Command cmd) {
	connect();
	Protocol.sendCommand(outputStream, cmd, new byte[0][]);
	pipelinedCommands++;
	return this;
	}

	protected abstract void connect();
	
	/* (non-Javadoc)
	 * @see redis.clients.jedis.ConnectionInterface#close()
	 */
	@Override
	public void close() {
		disconnect();
	}

	/* (non-Javadoc)
	 * @see redis.clients.jedis.ConnectionInterface#disconnect()
	 */
	@Override
	public void disconnect() {
	if (isConnected()) {
	    try {
		inputStream.close();
		outputStream.close();
		if (!socket.isClosed()) {
		    socket.close();
		}
	    } catch (IOException ex) {
		throw new JedisConnectionException(ex);
	    }
	}
	}

	/* (non-Javadoc)
	 * @see redis.clients.jedis.ConnectionInterface#isConnected()
	 */
	@Override
	public boolean isConnected() {
	return socket != null && socket.isBound() && !socket.isClosed()
		&& socket.isConnected() && !socket.isInputShutdown()
		&& !socket.isOutputShutdown();
	}

	protected String getStatusCodeReply() {
	flush();
	pipelinedCommands--;
	final byte[] resp = (byte[]) Protocol.read(inputStream);
	if (null == resp) {
	    return null;
	} else {
	    return SafeEncoder.encode(resp);
	}
	}

	/* (non-Javadoc)
	 * @see redis.clients.jedis.ConnectionInterface#getBulkReply()
	 */
	@Override
	public String getBulkReply() {
	final byte[] result = getBinaryBulkReply();
	if (null != result) {
	    return SafeEncoder.encode(result);
	} else {
	    return null;
	}
	}

	/* (non-Javadoc)
	 * @see redis.clients.jedis.ConnectionInterface#getBinaryBulkReply()
	 */
	@Override
	public byte[] getBinaryBulkReply() {
	flush();
	pipelinedCommands--;
	return (byte[]) Protocol.read(inputStream);
	}

	/* (non-Javadoc)
	 * @see redis.clients.jedis.ConnectionInterface#getIntegerReply()
	 */
	@Override
	public Long getIntegerReply() {
	flush();
	pipelinedCommands--;
	return (Long) Protocol.read(inputStream);
	}

	/* (non-Javadoc)
	 * @see redis.clients.jedis.ConnectionInterface#getMultiBulkReply()
	 */
	@Override
	public List<String> getMultiBulkReply() {
	return BuilderFactory.STRING_LIST.build(getBinaryMultiBulkReply());
	}

	/* (non-Javadoc)
	 * @see redis.clients.jedis.ConnectionInterface#getBinaryMultiBulkReply()
	 */
	@Override
	@SuppressWarnings("unchecked")
	public List<byte[]> getBinaryMultiBulkReply() {
	flush();
	pipelinedCommands--;
	return (List<byte[]>) Protocol.read(inputStream);
	}

	/* (non-Javadoc)
	 * @see redis.clients.jedis.ConnectionInterface#resetPipelinedCount()
	 */
	@Override
	public void resetPipelinedCount() {
	    pipelinedCommands = 0;
	}

	/* (non-Javadoc)
	 * @see redis.clients.jedis.ConnectionInterface#getRawObjectMultiBulkReply()
	 */
	@Override
	@SuppressWarnings("unchecked")
	public List<Object> getRawObjectMultiBulkReply() {
	    return (List<Object>) Protocol.read(inputStream);
	}

	/* (non-Javadoc)
	 * @see redis.clients.jedis.ConnectionInterface#getObjectMultiBulkReply()
	 */
	@Override
	public List<Object> getObjectMultiBulkReply() {
	    flush();
	    pipelinedCommands--;
	    return getRawObjectMultiBulkReply();
	}

	/* (non-Javadoc)
	 * @see redis.clients.jedis.ConnectionInterface#getIntegerMultiBulkReply()
	 */
	@Override
	@SuppressWarnings("unchecked")
	public List<Long> getIntegerMultiBulkReply() {
	flush();
	pipelinedCommands--;
	return (List<Long>) Protocol.read(inputStream);
	}

	/* (non-Javadoc)
	 * @see redis.clients.jedis.ConnectionInterface#getAll()
	 */
	@Override
	public List<Object> getAll() {
	return getAll(0);
	}

	/* (non-Javadoc)
	 * @see redis.clients.jedis.ConnectionInterface#getAll(int)
	 */
	@Override
	public List<Object> getAll(int except) {
	List<Object> all = new ArrayList<Object>();
	flush();
	while (pipelinedCommands > except) {
	    try {
		all.add(Protocol.read(inputStream));
	    } catch (JedisDataException e) {
		all.add(e);
	    }
	    pipelinedCommands--;
	}
	return all;
	}

	/* (non-Javadoc)
	 * @see redis.clients.jedis.ConnectionInterface#getOne()
	 */
	@Override
	public Object getOne() {
	flush();
	pipelinedCommands--;
	return Protocol.read(inputStream);
	}

	/* (non-Javadoc)
	 * @see redis.clients.jedis.ConnectionInterface#setTimeout(int)
	 */
	@Override
	public abstract void setTimeout(int timeout);
	
}