package redis.clients.jedis;

import static redis.clients.jedis.Protocol.toByteArray;
import static redis.clients.jedis.Protocol.Command.*;
import static redis.clients.jedis.Protocol.Keyword.ENCODING;
import static redis.clients.jedis.Protocol.Keyword.IDLETIME;
import static redis.clients.jedis.Protocol.Keyword.LEN;
import static redis.clients.jedis.Protocol.Keyword.LIMIT;
import static redis.clients.jedis.Protocol.Keyword.NO;
import static redis.clients.jedis.Protocol.Keyword.ONE;
import static redis.clients.jedis.Protocol.Keyword.REFCOUNT;
import static redis.clients.jedis.Protocol.Keyword.RESET;
import static redis.clients.jedis.Protocol.Keyword.STORE;
import static redis.clients.jedis.Protocol.Keyword.WITHSCORES;

import java.net.Socket;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import redis.clients.jedis.Protocol.Command;
import redis.clients.jedis.Protocol.Keyword;
import redis.clients.util.SafeEncoder;

public class BinaryClient implements ConnectionInterface {
    public enum LIST_POSITION {
	BEFORE, AFTER;
	public final byte[] raw;

	private LIST_POSITION() {
	    raw = SafeEncoder.encode(name());
	}
    }

    private boolean isInMulti;

    private String password;

    private long db;

    private boolean isInWatch;

    private Connection connection;
    
    public boolean isInMulti() {
	return isInMulti;
    }

    public boolean isInWatch() {
	return isInWatch;
    }

    public BinaryClient(final String pathname) {
    connection = new UnixSocketConnection(pathname);
    }    

    public BinaryClient(final String host, final int port) {
    connection = new TcpConnection(host, port);
    }

	public void setTimeout(int timeout) {
	connection.setTimeout(timeout);
	}
	
    protected byte[][] joinParameters(byte[] first, byte[][] rest) {
	byte[][] result = new byte[rest.length + 1][];
	result[0] = first;
	for (int i = 0; i < rest.length; i++) {
	    result[i + 1] = rest[i];
	}
	return result;
    }

    public void setPassword(final String password) {
	this.password = password;
    }

    public void connect() {
	if (!connection.isConnected()) {
		connection.connect();
	    if (password != null) {
		auth(password);
		getStatusCodeReply();
	    }
	    if (db > 0) {
		select(Long.valueOf(db).intValue());
		getStatusCodeReply();
	    }
	}
    }

	protected String getStatusCodeReply() {
	return connection.getStatusCodeReply();
	}

	public void ping() {
	connection.sendCommand(Command.PING);
    }

    public void set(final byte[] key, final byte[] value) {
    connection.sendCommand(Command.SET, key, value);
    }

    public void set(final byte[] key, final byte[] value, final byte[] nxxx,
	    final byte[] expx, final long time) {
    connection.sendCommand(Command.SET, key, value, nxxx, expx, toByteArray(time));
    }

    public void get(final byte[] key) {
    connection.sendCommand(Command.GET, key);
    }

    public void quit() {
	db = 0;
	connection.sendCommand(QUIT);
    }

    public void exists(final byte[] key) {
    connection.sendCommand(EXISTS, key);
    }

    public void del(final byte[]... keys) {
    connection.sendCommand(DEL, keys);
    }

    public void type(final byte[] key) {
    connection.sendCommand(TYPE, key);
    }

    public void flushDB() {
   	connection.sendCommand(FLUSHDB);
    }

    public void keys(final byte[] pattern) {
    connection.sendCommand(KEYS, pattern);
    }

    public void randomKey() {
    connection.sendCommand(RANDOMKEY);
    }

    public void rename(final byte[] oldkey, final byte[] newkey) {
    connection.sendCommand(RENAME, oldkey, newkey);
    }

    public void renamenx(final byte[] oldkey, final byte[] newkey) {
    connection.sendCommand(RENAMENX, oldkey, newkey);
    }

    public void dbSize() {
    connection.sendCommand(DBSIZE);
    }

    public void expire(final byte[] key, final int seconds) {
    connection.sendCommand(EXPIRE, key, toByteArray(seconds));
    }

    public void expireAt(final byte[] key, final long unixTime) {
    connection.sendCommand(EXPIREAT, key, toByteArray(unixTime));
    }

    public void ttl(final byte[] key) {
    connection.sendCommand(TTL, key);
    }

    public void select(final int index) {
	db = index;
	connection.sendCommand(SELECT, toByteArray(index));
    }

    public void move(final byte[] key, final int dbIndex) {
    connection.sendCommand(MOVE, key, toByteArray(dbIndex));
    }

    public void flushAll() {
    connection.sendCommand(FLUSHALL);
    }

    public void getSet(final byte[] key, final byte[] value) {
    connection.sendCommand(GETSET, key, value);
    }

    public void mget(final byte[]... keys) {
    connection.sendCommand(MGET, keys);
    }

    public void setnx(final byte[] key, final byte[] value) {
    connection.sendCommand(SETNX, key, value);
    }

    public void setex(final byte[] key, final int seconds, final byte[] value) {
	connection.sendCommand(SETEX, key, toByteArray(seconds), value);
    }

    public void mset(final byte[]... keysvalues) {
	connection.sendCommand(MSET, keysvalues);
    }

    public void msetnx(final byte[]... keysvalues) {
	connection.sendCommand(MSETNX, keysvalues);
    }

    public void decrBy(final byte[] key, final long integer) {
	connection.sendCommand(DECRBY, key, toByteArray(integer));
    }

    public void decr(final byte[] key) {
	connection.sendCommand(DECR, key);
    }

    public void incrBy(final byte[] key, final long integer) {
	connection.sendCommand(INCRBY, key, toByteArray(integer));
    }

    public void incr(final byte[] key) {
	connection.sendCommand(INCR, key);
    }

    public void append(final byte[] key, final byte[] value) {
	connection.sendCommand(APPEND, key, value);
    }

    public void substr(final byte[] key, final int start, final int end) {
	connection.sendCommand(SUBSTR, key, toByteArray(start), toByteArray(end));
    }

    public void hset(final byte[] key, final byte[] field, final byte[] value) {
	connection.sendCommand(HSET, key, field, value);
    }

    public void hget(final byte[] key, final byte[] field) {
	connection.sendCommand(HGET, key, field);
    }

    public void hsetnx(final byte[] key, final byte[] field, final byte[] value) {
	connection.sendCommand(HSETNX, key, field, value);
    }

    public void hmset(final byte[] key, final Map<byte[], byte[]> hash) {
	final List<byte[]> params = new ArrayList<byte[]>();
	params.add(key);

	for (final Entry<byte[], byte[]> entry : hash.entrySet()) {
	    params.add(entry.getKey());
	    params.add(entry.getValue());
	}
	connection.sendCommand(HMSET, params.toArray(new byte[params.size()][]));
    }

    public void hmget(final byte[] key, final byte[]... fields) {
	final byte[][] params = new byte[fields.length + 1][];
	params[0] = key;
	System.arraycopy(fields, 0, params, 1, fields.length);
	connection.sendCommand(HMGET, params);
    }

    public void hincrBy(final byte[] key, final byte[] field, final long value) {
	connection.sendCommand(HINCRBY, key, field, toByteArray(value));
    }

    public void hexists(final byte[] key, final byte[] field) {
	connection.sendCommand(HEXISTS, key, field);
    }

    public void hdel(final byte[] key, final byte[]... fields) {
	connection.sendCommand(HDEL, joinParameters(key, fields));
    }

    public void hlen(final byte[] key) {
	connection.sendCommand(HLEN, key);
    }

    public void hkeys(final byte[] key) {
	connection.sendCommand(HKEYS, key);
    }

    public void hvals(final byte[] key) {
	connection.sendCommand(HVALS, key);
    }

    public void hgetAll(final byte[] key) {
	connection.sendCommand(HGETALL, key);
    }

    public void rpush(final byte[] key, final byte[]... strings) {
	connection.sendCommand(RPUSH, joinParameters(key, strings));
    }

    public void lpush(final byte[] key, final byte[]... strings) {
	connection.sendCommand(LPUSH, joinParameters(key, strings));
    }

    public void llen(final byte[] key) {
	connection.sendCommand(LLEN, key);
    }

    public void lrange(final byte[] key, final long start, final long end) {
	connection.sendCommand(LRANGE, key, toByteArray(start), toByteArray(end));
    }

    public void ltrim(final byte[] key, final long start, final long end) {
	connection.sendCommand(LTRIM, key, toByteArray(start), toByteArray(end));
    }

    public void lindex(final byte[] key, final long index) {
	connection.sendCommand(LINDEX, key, toByteArray(index));
    }

    public void lset(final byte[] key, final long index, final byte[] value) {
	connection.sendCommand(LSET, key, toByteArray(index), value);
    }

    public void lrem(final byte[] key, long count, final byte[] value) {
	connection.sendCommand(LREM, key, toByteArray(count), value);
    }

    public void lpop(final byte[] key) {
	connection.sendCommand(LPOP, key);
    }

    public void rpop(final byte[] key) {
	connection.sendCommand(RPOP, key);
    }

    public void rpoplpush(final byte[] srckey, final byte[] dstkey) {
	connection.sendCommand(RPOPLPUSH, srckey, dstkey);
    }

    public void sadd(final byte[] key, final byte[]... members) {
	connection.sendCommand(SADD, joinParameters(key, members));
    }

    public void smembers(final byte[] key) {
	connection.sendCommand(SMEMBERS, key);
    }

    public void srem(final byte[] key, final byte[]... members) {
	connection.sendCommand(SREM, joinParameters(key, members));
    }

    public void spop(final byte[] key) {
	connection.sendCommand(SPOP, key);
    }

    public void smove(final byte[] srckey, final byte[] dstkey,
	    final byte[] member) {
	connection.sendCommand(SMOVE, srckey, dstkey, member);
    }

    public void scard(final byte[] key) {
	connection.sendCommand(SCARD, key);
    }

    public void sismember(final byte[] key, final byte[] member) {
	connection.sendCommand(SISMEMBER, key, member);
    }

    public void sinter(final byte[]... keys) {
	connection.sendCommand(SINTER, keys);
    }

    public void sinterstore(final byte[] dstkey, final byte[]... keys) {
	final byte[][] params = new byte[keys.length + 1][];
	params[0] = dstkey;
	System.arraycopy(keys, 0, params, 1, keys.length);
	connection.sendCommand(SINTERSTORE, params);
    }

    public void sunion(final byte[]... keys) {
	connection.sendCommand(SUNION, keys);
    }

    public void sunionstore(final byte[] dstkey, final byte[]... keys) {
	byte[][] params = new byte[keys.length + 1][];
	params[0] = dstkey;
	System.arraycopy(keys, 0, params, 1, keys.length);
	connection.sendCommand(SUNIONSTORE, params);
    }

    public void sdiff(final byte[]... keys) {
	connection.sendCommand(SDIFF, keys);
    }

    public void sdiffstore(final byte[] dstkey, final byte[]... keys) {
	byte[][] params = new byte[keys.length + 1][];
	params[0] = dstkey;
	System.arraycopy(keys, 0, params, 1, keys.length);
	connection.sendCommand(SDIFFSTORE, params);
    }

    public void srandmember(final byte[] key) {
	connection.sendCommand(SRANDMEMBER, key);
    }

    public void zadd(final byte[] key, final double score, final byte[] member) {
	connection.sendCommand(ZADD, key, toByteArray(score), member);
    }

    public void zaddBinary(final byte[] key,
	    final Map<byte[], Double> scoreMembers) {

	ArrayList<byte[]> args = new ArrayList<byte[]>(
		scoreMembers.size() * 2 + 1);
	args.add(key);

	for (Map.Entry<byte[], Double> entry : scoreMembers.entrySet()) {
	    args.add(toByteArray(entry.getValue()));
	    args.add(entry.getKey());
	}

	byte[][] argsArray = new byte[args.size()][];
	args.toArray(argsArray);

	connection.sendCommand(ZADD, argsArray);
    }

    public void zrange(final byte[] key, final long start, final long end) {
	connection.sendCommand(ZRANGE, key, toByteArray(start), toByteArray(end));
    }

    public void zrem(final byte[] key, final byte[]... members) {
	connection.sendCommand(ZREM, joinParameters(key, members));
    }

    public void zincrby(final byte[] key, final double score,
	    final byte[] member) {
	connection.sendCommand(ZINCRBY, key, toByteArray(score), member);
    }

    public void zrank(final byte[] key, final byte[] member) {
	connection.sendCommand(ZRANK, key, member);
    }

    public void zrevrank(final byte[] key, final byte[] member) {
	connection.sendCommand(ZREVRANK, key, member);
    }

    public void zrevrange(final byte[] key, final long start, final long end) {
	connection.sendCommand(ZREVRANGE, key, toByteArray(start), toByteArray(end));
    }

    public void zrangeWithScores(final byte[] key, final long start,
	    final long end) {
	connection.sendCommand(ZRANGE, key, toByteArray(start), toByteArray(end),
		WITHSCORES.raw);
    }

    public void zrevrangeWithScores(final byte[] key, final long start,
	    final long end) {
	connection.sendCommand(ZREVRANGE, key, toByteArray(start), toByteArray(end),
		WITHSCORES.raw);
    }

    public void zcard(final byte[] key) {
	connection.sendCommand(ZCARD, key);
    }

    public void zscore(final byte[] key, final byte[] member) {
	connection.sendCommand(ZSCORE, key, member);
    }

    public void multi() {
	connection.sendCommand(MULTI);
	isInMulti = true;
    }

    public void discard() {
	connection.sendCommand(DISCARD);
	isInMulti = false;
	isInWatch = false;
    }

    public void exec() {
	connection.sendCommand(EXEC);
	isInMulti = false;
	isInWatch = false;
    }

    public void watch(final byte[]... keys) {
	connection.sendCommand(WATCH, keys);
	isInWatch = true;
    }

    public void unwatch() {
	connection.sendCommand(UNWATCH);
	isInWatch = false;
    }

    public void sort(final byte[] key) {
	connection.sendCommand(SORT, key);
    }

    public void sort(final byte[] key, final SortingParams sortingParameters) {
	final List<byte[]> args = new ArrayList<byte[]>();
	args.add(key);
	args.addAll(sortingParameters.getParams());
	connection.sendCommand(SORT, args.toArray(new byte[args.size()][]));
    }

    public void blpop(final byte[][] args) {
	connection.sendCommand(BLPOP, args);
    }

    public void blpop(final int timeout, final byte[]... keys) {
	final List<byte[]> args = new ArrayList<byte[]>();
	for (final byte[] arg : keys) {
	    args.add(arg);
	}
	args.add(Protocol.toByteArray(timeout));
	blpop(args.toArray(new byte[args.size()][]));
    }

    public void sort(final byte[] key, final SortingParams sortingParameters,
	    final byte[] dstkey) {
	final List<byte[]> args = new ArrayList<byte[]>();
	args.add(key);
	args.addAll(sortingParameters.getParams());
	args.add(STORE.raw);
	args.add(dstkey);
	connection.sendCommand(SORT, args.toArray(new byte[args.size()][]));
    }

    public void sort(final byte[] key, final byte[] dstkey) {
	connection.sendCommand(SORT, key, STORE.raw, dstkey);
    }

    public void brpop(final byte[][] args) {
	connection.sendCommand(BRPOP, args);
    }

    public void brpop(final int timeout, final byte[]... keys) {
	final List<byte[]> args = new ArrayList<byte[]>();
	for (final byte[] arg : keys) {
	    args.add(arg);
	}
	args.add(Protocol.toByteArray(timeout));
	brpop(args.toArray(new byte[args.size()][]));
    }

    public void auth(final String password) {
	setPassword(password);
	connection.sendCommand(AUTH, password);
    }

    public void subscribe(final byte[]... channels) {
	connection.sendCommand(SUBSCRIBE, channels);
    }

    public void publish(final byte[] channel, final byte[] message) {
	connection.sendCommand(PUBLISH, channel, message);
    }

    public void unsubscribe() {
	connection.sendCommand(UNSUBSCRIBE);
    }

    public void unsubscribe(final byte[]... channels) {
	connection.sendCommand(UNSUBSCRIBE, channels);
    }

    public void psubscribe(final byte[]... patterns) {
	connection.sendCommand(PSUBSCRIBE, patterns);
    }

    public void punsubscribe() {
	connection.sendCommand(PUNSUBSCRIBE);
    }

    public void punsubscribe(final byte[]... patterns) {
	connection.sendCommand(PUNSUBSCRIBE, patterns);
    }
    
    public void pubsub(final byte[]... args) {
    	connection.sendCommand(PUBSUB, args);
    }
    public void zcount(final byte[] key, final double min, final double max) {

	byte byteArrayMin[] = (min == Double.NEGATIVE_INFINITY) ? "-inf"
		.getBytes() : toByteArray(min);
	byte byteArrayMax[] = (max == Double.POSITIVE_INFINITY) ? "+inf"
		.getBytes() : toByteArray(max);

	connection.sendCommand(ZCOUNT, key, byteArrayMin, byteArrayMax);
    }

    public void zcount(final byte[] key, final byte min[], final byte max[]) {
	connection.sendCommand(ZCOUNT, key, min, max);
    }

    public void zcount(final byte[] key, final String min, final String max) {
	connection.sendCommand(ZCOUNT, key, min.getBytes(), max.getBytes());
    }

    public void zrangeByScore(final byte[] key, final double min,
	    final double max) {

	byte byteArrayMin[] = (min == Double.NEGATIVE_INFINITY) ? "-inf"
		.getBytes() : toByteArray(min);
	byte byteArrayMax[] = (max == Double.POSITIVE_INFINITY) ? "+inf"
		.getBytes() : toByteArray(max);

	connection.sendCommand(ZRANGEBYSCORE, key, byteArrayMin, byteArrayMax);
    }

    public void zrangeByScore(final byte[] key, final byte[] min,
	    final byte[] max) {
	connection.sendCommand(ZRANGEBYSCORE, key, min, max);
    }

    public void zrangeByScore(final byte[] key, final String min,
	    final String max) {
	connection.sendCommand(ZRANGEBYSCORE, key, min.getBytes(), max.getBytes());
    }

    public void zrevrangeByScore(final byte[] key, final double max,
	    final double min) {

	byte byteArrayMin[] = (min == Double.NEGATIVE_INFINITY) ? "-inf"
		.getBytes() : toByteArray(min);
	byte byteArrayMax[] = (max == Double.POSITIVE_INFINITY) ? "+inf"
		.getBytes() : toByteArray(max);

	connection.sendCommand(ZREVRANGEBYSCORE, key, byteArrayMax, byteArrayMin);
    }

    public void zrevrangeByScore(final byte[] key, final byte[] max,
	    final byte[] min) {
	connection.sendCommand(ZREVRANGEBYSCORE, key, max, min);
    }

    public void zrevrangeByScore(final byte[] key, final String max,
	    final String min) {
	connection.sendCommand(ZREVRANGEBYSCORE, key, max.getBytes(), min.getBytes());
    }

    public void zrangeByScore(final byte[] key, final double min,
	    final double max, final int offset, int count) {

	byte byteArrayMin[] = (min == Double.NEGATIVE_INFINITY) ? "-inf"
		.getBytes() : toByteArray(min);
	byte byteArrayMax[] = (max == Double.POSITIVE_INFINITY) ? "+inf"
		.getBytes() : toByteArray(max);

	connection.sendCommand(ZRANGEBYSCORE, key, byteArrayMin, byteArrayMax, LIMIT.raw,
		toByteArray(offset), toByteArray(count));
    }

    public void zrangeByScore(final byte[] key, final String min,
	    final String max, final int offset, int count) {

	connection.sendCommand(ZRANGEBYSCORE, key, min.getBytes(), max.getBytes(),
		LIMIT.raw, toByteArray(offset), toByteArray(count));
    }

    public void zrevrangeByScore(final byte[] key, final double max,
	    final double min, final int offset, int count) {

	byte byteArrayMin[] = (min == Double.NEGATIVE_INFINITY) ? "-inf"
		.getBytes() : toByteArray(min);
	byte byteArrayMax[] = (max == Double.POSITIVE_INFINITY) ? "+inf"
		.getBytes() : toByteArray(max);

	connection.sendCommand(ZREVRANGEBYSCORE, key, byteArrayMax, byteArrayMin,
		LIMIT.raw, toByteArray(offset), toByteArray(count));
    }

    public void zrevrangeByScore(final byte[] key, final String max,
	    final String min, final int offset, int count) {

	connection.sendCommand(ZREVRANGEBYSCORE, key, max.getBytes(), min.getBytes(),
		LIMIT.raw, toByteArray(offset), toByteArray(count));
    }

    public void zrangeByScoreWithScores(final byte[] key, final double min,
	    final double max) {

	byte byteArrayMin[] = (min == Double.NEGATIVE_INFINITY) ? "-inf"
		.getBytes() : toByteArray(min);
	byte byteArrayMax[] = (max == Double.POSITIVE_INFINITY) ? "+inf"
		.getBytes() : toByteArray(max);

	connection.sendCommand(ZRANGEBYSCORE, key, byteArrayMin, byteArrayMax,
		WITHSCORES.raw);
    }

    public void zrangeByScoreWithScores(final byte[] key, final String min,
	    final String max) {

	connection.sendCommand(ZRANGEBYSCORE, key, min.getBytes(), max.getBytes(),
		WITHSCORES.raw);
    }

    public void zrevrangeByScoreWithScores(final byte[] key, final double max,
	    final double min) {

	byte byteArrayMin[] = (min == Double.NEGATIVE_INFINITY) ? "-inf"
		.getBytes() : toByteArray(min);
	byte byteArrayMax[] = (max == Double.POSITIVE_INFINITY) ? "+inf"
		.getBytes() : toByteArray(max);

	connection.sendCommand(ZREVRANGEBYSCORE, key, byteArrayMax, byteArrayMin,
		WITHSCORES.raw);
    }

    public void zrevrangeByScoreWithScores(final byte[] key, final String max,
	    final String min) {
	connection.sendCommand(ZREVRANGEBYSCORE, key, max.getBytes(), min.getBytes(),
		WITHSCORES.raw);
    }

    public void zrangeByScoreWithScores(final byte[] key, final double min,
	    final double max, final int offset, final int count) {

	byte byteArrayMin[] = (min == Double.NEGATIVE_INFINITY) ? "-inf"
		.getBytes() : toByteArray(min);
	byte byteArrayMax[] = (max == Double.POSITIVE_INFINITY) ? "+inf"
		.getBytes() : toByteArray(max);

	connection.sendCommand(ZRANGEBYSCORE, key, byteArrayMin, byteArrayMax, LIMIT.raw,
		toByteArray(offset), toByteArray(count), WITHSCORES.raw);
    }

    public void zrangeByScoreWithScores(final byte[] key, final String min,
	    final String max, final int offset, final int count) {
	connection.sendCommand(ZRANGEBYSCORE, key, min.getBytes(), max.getBytes(),
		LIMIT.raw, toByteArray(offset), toByteArray(count),
		WITHSCORES.raw);
    }

    public void zrevrangeByScoreWithScores(final byte[] key, final double max,
	    final double min, final int offset, final int count) {

	byte byteArrayMin[] = (min == Double.NEGATIVE_INFINITY) ? "-inf"
		.getBytes() : toByteArray(min);
	byte byteArrayMax[] = (max == Double.POSITIVE_INFINITY) ? "+inf"
		.getBytes() : toByteArray(max);

	connection.sendCommand(ZREVRANGEBYSCORE, key, byteArrayMax, byteArrayMin,
		LIMIT.raw, toByteArray(offset), toByteArray(count),
		WITHSCORES.raw);
    }

    public void zrevrangeByScoreWithScores(final byte[] key, final String max,
	    final String min, final int offset, final int count) {

	connection.sendCommand(ZREVRANGEBYSCORE, key, max.getBytes(), min.getBytes(),
		LIMIT.raw, toByteArray(offset), toByteArray(count),
		WITHSCORES.raw);
    }

    public void zrangeByScore(final byte[] key, final byte[] min,
	    final byte[] max, final int offset, int count) {
	connection.sendCommand(ZRANGEBYSCORE, key, min, max, LIMIT.raw,
		toByteArray(offset), toByteArray(count));
    }

    public void zrevrangeByScore(final byte[] key, final byte[] max,
	    final byte[] min, final int offset, int count) {
	connection.sendCommand(ZREVRANGEBYSCORE, key, max, min, LIMIT.raw,
		toByteArray(offset), toByteArray(count));
    }

    public void zrangeByScoreWithScores(final byte[] key, final byte[] min,
	    final byte[] max) {
	connection.sendCommand(ZRANGEBYSCORE, key, min, max, WITHSCORES.raw);
    }

    public void zrevrangeByScoreWithScores(final byte[] key, final byte[] max,
	    final byte[] min) {
	connection.sendCommand(ZREVRANGEBYSCORE, key, max, min, WITHSCORES.raw);
    }

    public void zrangeByScoreWithScores(final byte[] key, final byte[] min,
	    final byte[] max, final int offset, final int count) {
	connection.sendCommand(ZRANGEBYSCORE, key, min, max, LIMIT.raw,
		toByteArray(offset), toByteArray(count), WITHSCORES.raw);
    }

    public void zrevrangeByScoreWithScores(final byte[] key, final byte[] max,
	    final byte[] min, final int offset, final int count) {
	connection.sendCommand(ZREVRANGEBYSCORE, key, max, min, LIMIT.raw,
		toByteArray(offset), toByteArray(count), WITHSCORES.raw);
    }

    public void zremrangeByRank(final byte[] key, final long start,
	    final long end) {
	connection.sendCommand(ZREMRANGEBYRANK, key, toByteArray(start), toByteArray(end));
    }

    public void zremrangeByScore(final byte[] key, final byte[] start,
	    final byte[] end) {
	connection.sendCommand(ZREMRANGEBYSCORE, key, start, end);
    }

    public void zremrangeByScore(final byte[] key, final String start,
	    final String end) {
	connection.sendCommand(ZREMRANGEBYSCORE, key, start.getBytes(), end.getBytes());
    }

    public void zunionstore(final byte[] dstkey, final byte[]... sets) {
	final byte[][] params = new byte[sets.length + 2][];
	params[0] = dstkey;
	params[1] = toByteArray(sets.length);
	System.arraycopy(sets, 0, params, 2, sets.length);
	connection.sendCommand(ZUNIONSTORE, params);
    }

    public void zunionstore(final byte[] dstkey, final ZParams params,
	    final byte[]... sets) {
	final List<byte[]> args = new ArrayList<byte[]>();
	args.add(dstkey);
	args.add(Protocol.toByteArray(sets.length));
	for (final byte[] set : sets) {
	    args.add(set);
	}
	args.addAll(params.getParams());
	connection.sendCommand(ZUNIONSTORE, args.toArray(new byte[args.size()][]));
    }

    public void zinterstore(final byte[] dstkey, final byte[]... sets) {
	final byte[][] params = new byte[sets.length + 2][];
	params[0] = dstkey;
	params[1] = Protocol.toByteArray(sets.length);
	System.arraycopy(sets, 0, params, 2, sets.length);
	connection.sendCommand(ZINTERSTORE, params);
    }

    public void zinterstore(final byte[] dstkey, final ZParams params,
	    final byte[]... sets) {
	final List<byte[]> args = new ArrayList<byte[]>();
	args.add(dstkey);
	args.add(Protocol.toByteArray(sets.length));
	for (final byte[] set : sets) {
	    args.add(set);
	}
	args.addAll(params.getParams());
	connection.sendCommand(ZINTERSTORE, args.toArray(new byte[args.size()][]));
    }

    public void save() {
	connection.sendCommand(SAVE);
    }

    public void bgsave() {
	connection.sendCommand(BGSAVE);
    }

    public void bgrewriteaof() {
	connection.sendCommand(BGREWRITEAOF);
    }

    public void lastsave() {
	connection.sendCommand(LASTSAVE);
    }

    public void shutdown() {
	connection.sendCommand(SHUTDOWN);
    }

    public void info() {
	connection.sendCommand(INFO);
    }

    public void info(final String section) {
	connection.sendCommand(INFO, section);
    }

    public void monitor() {
	connection.sendCommand(MONITOR);
    }

    public void slaveof(final String host, final int port) {
	connection.sendCommand(SLAVEOF, host, String.valueOf(port));
    }

    public void slaveofNoOne() {
	connection.sendCommand(SLAVEOF, NO.raw, ONE.raw);
    }

    public void configGet(final byte[] pattern) {
	connection.sendCommand(CONFIG, Keyword.GET.raw, pattern);
    }

    public void configSet(final byte[] parameter, final byte[] value) {
	connection.sendCommand(CONFIG, Keyword.SET.raw, parameter, value);
    }

    public void strlen(final byte[] key) {
	connection.sendCommand(STRLEN, key);
    }

    public void sync() {
	connection.sendCommand(SYNC);
    }

    public void lpushx(final byte[] key, final byte[]... string) {
	connection.sendCommand(LPUSHX, joinParameters(key, string));
    }

    public void persist(final byte[] key) {
	connection.sendCommand(PERSIST, key);
    }

    public void rpushx(final byte[] key, final byte[]... string) {
	connection.sendCommand(RPUSHX, joinParameters(key, string));
    }

    public void echo(final byte[] string) {
	connection.sendCommand(ECHO, string);
    }

    public void linsert(final byte[] key, final LIST_POSITION where,
	    final byte[] pivot, final byte[] value) {
	connection.sendCommand(LINSERT, key, where.raw, pivot, value);
    }

    public void debug(final DebugParams params) {
	connection.sendCommand(DEBUG, params.getCommand());
    }

    public void brpoplpush(final byte[] source, final byte[] destination,
	    final int timeout) {
	connection.sendCommand(BRPOPLPUSH, source, destination, toByteArray(timeout));
    }

    public void configResetStat() {
	connection.sendCommand(CONFIG, Keyword.RESETSTAT.name());
    }

    public void setbit(byte[] key, long offset, byte[] value) {
	connection.sendCommand(SETBIT, key, toByteArray(offset), value);
    }

    public void setbit(byte[] key, long offset, boolean value) {
	connection.sendCommand(SETBIT, key, toByteArray(offset), toByteArray(value));
    }

    public void getbit(byte[] key, long offset) {
	connection.sendCommand(GETBIT, key, toByteArray(offset));
    }

    public void setrange(byte[] key, long offset, byte[] value) {
	connection.sendCommand(SETRANGE, key, toByteArray(offset), value);
    }

    public void getrange(byte[] key, long startOffset, long endOffset) {
	connection.sendCommand(GETRANGE, key, toByteArray(startOffset),
		toByteArray(endOffset));
    }

    public Long getDB() {
	return db;
    }

    public void disconnect() {
	db = 0;
	connection.disconnect();
    }

    public void close() {
	db = 0;
	connection.close();
    }

    public void resetState() {
	if (isInMulti())
	    discard();

	if (isInWatch())
	    unwatch();
    }

    protected void sendEvalCommand(Command command, byte[] script,
	    byte[] keyCount, byte[][] params) {

	final byte[][] allArgs = new byte[params.length + 2][];

	allArgs[0] = script;
	allArgs[1] = keyCount;

	for (int i = 0; i < params.length; i++)
	    allArgs[i + 2] = params[i];

	connection.sendCommand(command, allArgs);
    }

    public void eval(byte[] script, byte[] keyCount, byte[][] params) {
	sendEvalCommand(EVAL, script, keyCount, params);
    }

    public void eval(byte[] script, int keyCount, byte[]... params) {
	eval(script, toByteArray(keyCount), params);
    }

    public void evalsha(byte[] sha1, byte[] keyCount, byte[]... params) {
	sendEvalCommand(EVALSHA, sha1, keyCount, params);
    }

    public void evalsha(byte[] sha1, int keyCount, byte[]... params) {
	sendEvalCommand(EVALSHA, sha1, toByteArray(keyCount), params);
    }

    public void scriptFlush() {
	connection.sendCommand(SCRIPT, Keyword.FLUSH.raw);
    }

    public void scriptExists(byte[]... sha1) {
	byte[][] args = new byte[sha1.length + 1][];
	args[0] = Keyword.EXISTS.raw;
	for (int i = 0; i < sha1.length; i++)
	    args[i + 1] = sha1[i];

	connection.sendCommand(SCRIPT, args);
    }

    public void scriptLoad(byte[] script) {
	connection.sendCommand(SCRIPT, Keyword.LOAD.raw, script);
    }

    public void scriptKill() {
	connection.sendCommand(SCRIPT, Keyword.KILL.raw);
    }

    public void slowlogGet() {
	connection.sendCommand(SLOWLOG, Keyword.GET.raw);
    }

    public void slowlogGet(long entries) {
	connection.sendCommand(SLOWLOG, Keyword.GET.raw, toByteArray(entries));
    }

    public void slowlogReset() {
	connection.sendCommand(SLOWLOG, RESET.raw);
    }

    public void slowlogLen() {
	connection.sendCommand(SLOWLOG, LEN.raw);
    }

    public void objectRefcount(byte[] key) {
	connection.sendCommand(OBJECT, REFCOUNT.raw, key);
    }

    public void objectIdletime(byte[] key) {
	connection.sendCommand(OBJECT, IDLETIME.raw, key);
    }

    public void objectEncoding(byte[] key) {
	connection.sendCommand(OBJECT, ENCODING.raw, key);
    }

    public void bitcount(byte[] key) {
	connection.sendCommand(BITCOUNT, key);
    }

    public void bitcount(byte[] key, long start, long end) {
	connection.sendCommand(BITCOUNT, key, toByteArray(start), toByteArray(end));
    }

    public void bitop(BitOP op, byte[] destKey, byte[]... srcKeys) {
	Keyword kw = Keyword.AND;
	int len = srcKeys.length;
	switch (op) {
	case AND:
	    kw = Keyword.AND;
	    break;
	case OR:
	    kw = Keyword.OR;
	    break;
	case XOR:
	    kw = Keyword.XOR;
	    break;
	case NOT:
	    kw = Keyword.NOT;
	    len = Math.min(1, len);
	    break;
	}

	byte[][] bargs = new byte[len + 2][];
	bargs[0] = kw.raw;
	bargs[1] = destKey;
	for (int i = 0; i < len; ++i) {
	    bargs[i + 2] = srcKeys[i];
	}

	connection.sendCommand(BITOP, bargs);
    }

    public void sentinel(final byte[]... args) {
	connection.sendCommand(SENTINEL, args);
    }

    public void dump(final byte[] key) {
	connection.sendCommand(DUMP, key);
    }

    public void restore(final byte[] key, final int ttl,
	    final byte[] serializedValue) {
	connection.sendCommand(RESTORE, key, toByteArray(ttl), serializedValue);
    }

    public void pexpire(final byte[] key, final int milliseconds) {
	connection.sendCommand(PEXPIRE, key, toByteArray(milliseconds));
    }

    public void pexpireAt(final byte[] key, final long millisecondsTimestamp) {
	connection.sendCommand(PEXPIREAT, key, toByteArray(millisecondsTimestamp));
    }

    public void pttl(final byte[] key) {
	connection.sendCommand(PTTL, key);
    }

    public void incrByFloat(final byte[] key, final double increment) {
	connection.sendCommand(INCRBYFLOAT, key, toByteArray(increment));
    }

    public void psetex(final byte[] key, final int milliseconds,
	    final byte[] value) {
	connection.sendCommand(PSETEX, key, toByteArray(milliseconds), value);
    }

    public void set(final byte[] key, final byte[] value, final byte[] nxxx) {
	connection.sendCommand(Command.SET, key, value, nxxx);
    }

    public void set(final byte[] key, final byte[] value, final byte[] nxxx,
	    final byte[] expx, final int time) {
	connection.sendCommand(Command.SET, key, value, nxxx, expx, toByteArray(time));
    }

    public void srandmember(final byte[] key, final int count) {
	connection.sendCommand(SRANDMEMBER, key, toByteArray(count));
    }

    public void clientKill(final byte[] client) {
	connection.sendCommand(CLIENT, Keyword.KILL.raw, client);
    }

    public void clientGetname() {
	connection.sendCommand(CLIENT, Keyword.GETNAME.raw);
    }

    public void clientList() {
	connection.sendCommand(CLIENT, Keyword.LIST.raw);
    }

    public void clientSetname(final byte[] name) {
	connection.sendCommand(CLIENT, Keyword.SETNAME.raw, name);
    }

    public void time() {
	connection.sendCommand(TIME);
    }

    public void migrate(final byte[] host, final int port, final byte[] key,
	    final int destinationDb, final int timeout) {
	connection.sendCommand(MIGRATE, host, toByteArray(port), key,
		toByteArray(destinationDb), toByteArray(timeout));
    }

    public void hincrByFloat(final byte[] key, final byte[] field,
	    double increment) {
	connection.sendCommand(HINCRBYFLOAT, key, field, toByteArray(increment));
    }

    @Deprecated
    /**
     * This method is deprecated due to bug (scan cursor should be unsigned long)
     * And will be removed on next major release
     * @see https://github.com/xetorthio/jedis/issues/531
     */
    public void scan(int cursor, final ScanParams params) {
	final List<byte[]> args = new ArrayList<byte[]>();
	args.add(toByteArray(cursor));
	args.addAll(params.getParams());
	connection.sendCommand(SCAN, args.toArray(new byte[args.size()][]));
    }

    @Deprecated
    /**
     * This method is deprecated due to bug (scan cursor should be unsigned long)
     * And will be removed on next major release
     * @see https://github.com/xetorthio/jedis/issues/531 
     */
    public void hscan(final byte[] key, int cursor, final ScanParams params) {
	final List<byte[]> args = new ArrayList<byte[]>();
	args.add(key);
	args.add(toByteArray(cursor));
	args.addAll(params.getParams());
	connection.sendCommand(HSCAN, args.toArray(new byte[args.size()][]));
    }
    
    @Deprecated
    /**
     * This method is deprecated due to bug (scan cursor should be unsigned long)
     * And will be removed on next major release
     * @see https://github.com/xetorthio/jedis/issues/531 
     */
    public void sscan(final byte[] key, int cursor, final ScanParams params) {
	final List<byte[]> args = new ArrayList<byte[]>();
	args.add(key);
	args.add(toByteArray(cursor));
	args.addAll(params.getParams());
	connection.sendCommand(SSCAN, args.toArray(new byte[args.size()][]));
    }
    
    @Deprecated
    /**
     * This method is deprecated due to bug (scan cursor should be unsigned long)
     * And will be removed on next major release
     * @see https://github.com/xetorthio/jedis/issues/531 
     */
    public void zscan(final byte[] key, int cursor, final ScanParams params) {
	final List<byte[]> args = new ArrayList<byte[]>();
	args.add(key);
	args.add(toByteArray(cursor));
	args.addAll(params.getParams());
	connection.sendCommand(ZSCAN, args.toArray(new byte[args.size()][]));
    }
    
    public void scan(final byte[] cursor, final ScanParams params) {
	final List<byte[]> args = new ArrayList<byte[]>();
	args.add(cursor);
	args.addAll(params.getParams());
	connection.sendCommand(SCAN, args.toArray(new byte[args.size()][]));
    }

    public void hscan(final byte[] key, final byte[] cursor, final ScanParams params) {
	final List<byte[]> args = new ArrayList<byte[]>();
	args.add(key);
	args.add(cursor);
	args.addAll(params.getParams());
	connection.sendCommand(HSCAN, args.toArray(new byte[args.size()][]));
    }

    public void sscan(final byte[] key, final byte[] cursor, final ScanParams params) {
	final List<byte[]> args = new ArrayList<byte[]>();
	args.add(key);
	args.add(cursor);
	args.addAll(params.getParams());
	connection.sendCommand(SSCAN, args.toArray(new byte[args.size()][]));
    }

    public void zscan(final byte[] key, final byte[] cursor, final ScanParams params) {
	final List<byte[]> args = new ArrayList<byte[]>();
	args.add(key);
	args.add(cursor);
	args.addAll(params.getParams());
	connection.sendCommand(ZSCAN, args.toArray(new byte[args.size()][]));
    }

    public void waitReplicas(int replicas, long timeout) {
	connection.sendCommand(WAIT, toByteArray(replicas), toByteArray(timeout));
    }

    public void cluster(final byte[]... args) {
	connection.sendCommand(CLUSTER, args);
    }

    public void asking() {
	connection.sendCommand(Command.ASKING);
    }

	@Override
	public int getTimeout() {
	return connection.getTimeout();
	}

	@Override
	public boolean isConnected() {
	return connection.isConnected();
	}

	@Override
	public String getBulkReply() {
	return connection.getBulkReply();
	}

	@Override
	public byte[] getBinaryBulkReply() {
	return connection.getBinaryBulkReply();
	}

	@Override
	public Long getIntegerReply() {
	return connection.getIntegerReply();
	}

	@Override
	public List<String> getMultiBulkReply() {
	return connection.getMultiBulkReply();
	}

	@Override
	public List<byte[]> getBinaryMultiBulkReply() {
	return connection.getBinaryMultiBulkReply();
	}

	@Override
	public void resetPipelinedCount() {
	connection.resetPipelinedCount();
	}

	@Override
	public List<Object> getRawObjectMultiBulkReply() {
	return connection.getRawObjectMultiBulkReply();
	}

	@Override
	public List<Object> getObjectMultiBulkReply() {
	return connection.getObjectMultiBulkReply();
	}

	@Override
	public List<Long> getIntegerMultiBulkReply() {
	return connection.getIntegerMultiBulkReply();
	}

	@Override
	public List<Object> getAll() {
	return connection.getAll();
	}

	@Override
	public List<Object> getAll(int except) {
	return connection.getAll(except);
	}

	@Override
	public Object getOne() {
	return connection.getOne();
	}

	@Override
	public void setTimeoutInfinite() {
	connection.setTimeoutInfinite();		
	}

	@Override
	public void rollbackTimeout() {
	connection.rollbackTimeout();	
	}

	@Override
	public Socket getSocket() {
	return connection.getSocket();
	}

	@Override
	public Connection sendCommand(final Command cmd, final String... args) {
	return connection.sendCommand(cmd, args);
	}

	@Override
	public Connection sendCommand(Command cmd, byte[]... args) {
	return connection.sendCommand(cmd, args);
	}

	@Override
	public Connection sendCommand(Command cmd) {
	return connection.sendCommand(cmd);
	}

	@Override
	public void flush() {
	connection.flush();
	}

	@Override
	public int getPort() {
	return connection.getPort();
	}

	@Override
	public String getHost() {
	return connection.getHost();
	}
}
