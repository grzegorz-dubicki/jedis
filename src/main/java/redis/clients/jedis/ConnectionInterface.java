package redis.clients.jedis;

import java.net.Socket;
import java.util.List;

import redis.clients.jedis.Protocol.Command;

public interface ConnectionInterface {

	public abstract int getPort();
	
	public abstract String getHost();
	
	public abstract Socket getSocket();

	public abstract void setTimeout(int timeout);
	
	public abstract void setTimeoutInfinite();
	
	public abstract void rollbackTimeout();
	
	public abstract int getTimeout();

	public abstract void close();

	public abstract void flush();
	
	public abstract void disconnect();

	public abstract boolean isConnected();

	public abstract String getBulkReply();

	public abstract byte[] getBinaryBulkReply();

	public abstract Long getIntegerReply();

	public abstract List<String> getMultiBulkReply();

	public abstract List<byte[]> getBinaryMultiBulkReply();

	public abstract void resetPipelinedCount();

	public abstract List<Object> getRawObjectMultiBulkReply();

	public abstract List<Object> getObjectMultiBulkReply();

	public abstract List<Long> getIntegerMultiBulkReply();

	public abstract List<Object> getAll();

	public abstract List<Object> getAll(int except);

	public abstract Object getOne();

	public abstract ConnectionInterface sendCommand(final Command cmd, final String... args);

	public abstract ConnectionInterface sendCommand(final Command cmd, final byte[]... args);

	public abstract ConnectionInterface sendCommand(final Command cmd);


}