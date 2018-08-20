package vegoo.redis.impl;

import java.util.Dictionary;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.osgi.service.cm.ConfigurationException;
import org.osgi.service.cm.ManagedService;
import org.osgi.service.component.annotations.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.BitPosParams;
import redis.clients.jedis.GeoCoordinate;
import redis.clients.jedis.GeoRadiusResponse;
import redis.clients.jedis.GeoUnit;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.SortingParams;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.params.geo.GeoRadiusParam;
import redis.clients.jedis.params.sortedset.ZAddParams;
import redis.clients.jedis.params.sortedset.ZIncrByParams;
import vegoo.redis.RedisService;


@Component (
		immediate = true, 
		configurationPid = "vegoo.redis",
		service = { RedisService.class,  ManagedService.class}, 
		property = {
		    "host=localhost",
		    "port=6398",
		    "password=foobared"
		} 
	)

public class RedisServiceImpl implements RedisService, ManagedService {
	private static final Logger logger = LoggerFactory.getLogger(RedisServiceImpl.class);

	static final String PN_HOST   = "host";
	static final String PN_PASSWORD   = "password";

	private String host;
	private String password;
	private Jedis jedis;
	
	@Override
	public void updated(Dictionary<String, ?> properties) throws ConfigurationException {
		this.host = (String) properties.get(PN_HOST);
		this.password = (String) properties.get(PN_PASSWORD);
		
		if(Strings.isNullOrEmpty(host)) {
			logger.error("没有在vegoo.redis.cfg文件中，配置Redis参数");
		}
		
		this.jedis = null;
	}
	
	
	
	private Jedis getJedis() {
        if(jedis==null) {
			jedis = new Jedis(host);
	        if(!Strings.isNullOrEmpty(password)) {
	        	jedis.auth("foobared");
	        }
	        logger.info("RedisService is ready.");
        }
		return jedis;
	}

	@Override
	public String set(String key, String value) {
		return getJedis().set(key, value);
	}

	@Override
	public String set(String key, String value, String nxxx, String expx, long time) {
		return getJedis().set(key, value, nxxx, expx, time);
	}

	@Override
	public String set(String key, String value, String nxxx) {
		return getJedis().set(key, value, nxxx);
	}

	@Override
	public String get(String key) {

		return getJedis().get(key);
	}

	@Override
	public Boolean exists(String key) {
		return getJedis().exists(key);
	}

	@Override
	public Long persist(String key) {
		
		return getJedis().persist(key);
	}

	@Override
	public String type(String key) {
		return getJedis().type(key);
	}

	@Override
	public Long expire(String key, int seconds) {
		return getJedis().expire(key, seconds);
	}

	@Override
	public Long pexpire(String key, long milliseconds) {
		
		return getJedis().pexpire(key, milliseconds);
	}

	@Override
	public Long expireAt(String key, long unixTime) {
		
		return getJedis().expireAt(key, unixTime);
	}

	@Override
	public Long pexpireAt(String key, long millisecondsTimestamp) {
		
		return getJedis().pexpireAt(key, millisecondsTimestamp);
	}

	@Override
	public Long ttl(String key) {
		
		return getJedis().ttl(key);
	}

	@Override
	public Long pttl(String key) {
		
		return getJedis().pttl(key);
	}

	@Override
	public Boolean setbit(String key, long offset, boolean value) {
		
		return getJedis().setbit(key, offset, value);
	}

	@Override
	public Boolean setbit(String key, long offset, String value) {
		
		return getJedis().setbit(key, offset, value);
	}

	@Override
	public Boolean getbit(String key, long offset) {
		
		return getJedis().getbit(key, offset);
	}

	@Override
	public Long setrange(String key, long offset, String value) {
		
		return getJedis().setrange(key, offset, value);
	}

	@Override
	public String getrange(String key, long startOffset, long endOffset) {
		
		return getJedis().getrange(key, startOffset, endOffset);
	}

	@Override
	public String getSet(String key, String value) {
		
		return getJedis().getSet(key, value);
	}

	@Override
	public Long setnx(String key, String value) {
		
		return getJedis().setnx(key, value);
	}

	@Override
	public String setex(String key, int seconds, String value) {
		
		return getJedis().setex(key, seconds, value);
	}

	@Override
	public String psetex(String key, long milliseconds, String value) {
		
		return getJedis().psetex(key, milliseconds, value);
	}

	@Override
	public Long decrBy(String key, long integer) {
		
		return getJedis().decrBy(key, integer);
	}

	@Override
	public Long decr(String key) {
		
		return getJedis().decr(key);
	}

	@Override
	public Long incrBy(String key, long integer) {
		
		return getJedis().incrBy(key, integer);
	}

	@Override
	public Double incrByFloat(String key, double value) {
		
		return getJedis().incrByFloat(key, value);
	}

	@Override
	public Long incr(String key) {
		
		return getJedis().incr(key);
	}

	@Override
	public Long append(String key, String value) {
		
		return getJedis().append(key, value);
	}

	@Override
	public String substr(String key, int start, int end) {
		
		return getJedis().substr(key, start, end);
	}

	@Override
	public Long hset(String key, String field, String value) {
		
		return getJedis().hset(key, field, value);
	}

	@Override
	public String hget(String key, String field) {
		
		return getJedis().hget(key, field);
	}

	@Override
	public Long hsetnx(String key, String field, String value) {
		
		return getJedis().hsetnx(key, field, value);
	}

	@Override
	public String hmset(String key, Map<String, String> hash) {
		
		return getJedis().hmset(key, hash);
	}

	@Override
	public List<String> hmget(String key, String... fields) {
		
		return getJedis().hmget(key, fields);
	}

	@Override
	public Long hincrBy(String key, String field, long value) {
		
		return getJedis().hincrBy(key, field, value);
	}

	@Override
	public Double hincrByFloat(String key, String field, double value) {
		
		return getJedis().hincrByFloat(key, field, value);
	}

	@Override
	public Boolean hexists(String key, String field) {
		
		return getJedis().hexists(key, field);
	}

	@Override
	public Long hdel(String key, String... field) {
		
		return getJedis().hdel(key, field);
	}

	@Override
	public Long hlen(String key) {
		
		return getJedis().hlen(key);
	}

	@Override
	public Set<String> hkeys(String key) {
		
		return getJedis().hkeys(key);
	}

	@Override
	public List<String> hvals(String key) {
		
		return getJedis().hvals(key);
	}

	@Override
	public Map<String, String> hgetAll(String key) {
		
		return getJedis().hgetAll(key);
	}

	@Override
	public Long rpush(String key, String... strings) {
		
		return getJedis().rpush(key, strings);
	}

	@Override
	public Long lpush(String key, String... strings) {
		
		return getJedis().lpush(key, strings);
	}

	@Override
	public Long llen(String key) {
		
		return getJedis().llen(key);
	}

	@Override
	public List<String> lrange(String key, long start, long end) {
		
		return getJedis().lrange(key, start, end);
	}

	@Override
	public String ltrim(String key, long start, long end) {
		
		return getJedis().ltrim(key, start, end);
	}

	@Override
	public String lindex(String key, long index) {
		
		return getJedis().lindex(key, index);
	}

	@Override
	public String lset(String key, long index, String value) {
		
		return getJedis().lset(key, index, value);
	}

	@Override
	public Long lrem(String key, long count, String value) {
		
		return getJedis().lrem(key, count, value);
	}

	@Override
	public String lpop(String key) {
		
		return getJedis().lpop(key);
	}

	@Override
	public String rpop(String key) {
		
		return getJedis().rpop(key);
	}

	@Override
	public Long sadd(String key, String... members) {
		
		return getJedis().sadd(key, members);
	}

	@Override
	public Set<String> smembers(String key) {
		
		return getJedis().smembers(key);
	}

	@Override
	public Long srem(String key, String... member) {
		
		return getJedis().srem(key, member);
		
	}

	@Override
	public String spop(String key) {
		
		return getJedis().spop(key);
	}

	@Override
	public Set<String> spop(String key, long count) {
		
		return getJedis().spop(key, count);
	}

	@Override
	public Long scard(String key) {
		
		return getJedis().scard(key);
	}

	@Override
	public Boolean sismember(String key, String member) {
		
		return getJedis().sismember(key, member);
	}

	@Override
	public String srandmember(String key) {
		
		return getJedis().srandmember(key);
	}

	@Override
	public List<String> srandmember(String key, int count) {
		
		return getJedis().srandmember(key, count);
	}

	@Override
	public Long strlen(String key) {
		
		return getJedis().strlen(key);
	}

	@Override
	public Long zadd(String key, double score, String member) {
		
		return getJedis().zadd(key, score, member);
	}

	@Override
	public Long zadd(String key, double score, String member, ZAddParams params) {
		
		return getJedis().zadd(key, score, member, params);
	}

	@Override
	public Long zadd(String key, Map<String, Double> scoreMembers) {
		
		return getJedis().zadd(key, scoreMembers);
	}

	@Override
	public Long zadd(String key, Map<String, Double> scoreMembers, ZAddParams params) {
		
		return getJedis().zadd(key, scoreMembers, params);
	}

	@Override
	public Set<String> zrange(String key, long start, long end) {
		
		return getJedis().zrange(key, start, end);
	}

	@Override
	public Long zrem(String key, String... members) {
		
		return getJedis().zrem(key, members);
	}

	@Override
	public Double zincrby(String key, double score, String member) {
		
		return getJedis().zincrby(key, score, member);
	}

	@Override
	public Double zincrby(String key, double score, String member, ZIncrByParams params) {
		
		return getJedis().zincrby(key, score, member, params);
	}

	@Override
	public Long zrank(String key, String member) {
		
		return getJedis().zrank(key, member);
	}

	@Override
	public Long zrevrank(String key, String member) {
		
		return getJedis().zrevrank(key, member);
	}

	@Override
	public Set<String> zrevrange(String key, long start, long end) {
		
		return getJedis().zrevrange(key, start, end);
	}

	@Override
	public Set<Tuple> zrangeWithScores(String key, long start, long end) {
		
		return getJedis().zrangeWithScores(key, start, end);
	}

	@Override
	public Set<Tuple> zrevrangeWithScores(String key, long start, long end) {
		
		return getJedis().zrevrangeWithScores(key, start, end);
	}

	@Override
	public Long zcard(String key) {
		
		return getJedis().zcard(key);
	}

	@Override
	public Double zscore(String key, String member) {
		
		return getJedis().zscore(key, member);
	}

	@Override
	public List<String> sort(String key) {
		
		return getJedis().sort(key);
	}

	@Override
	public List<String> sort(String key, SortingParams sortingParameters) {
		
		return getJedis().sort(key, sortingParameters);
	}

	@Override
	public Long zcount(String key, double min, double max) {
		
		return getJedis().zcount(key, min, max);
	}

	@Override
	public Long zcount(String key, String min, String max) {
		
		return getJedis().zcount(key, min, max);
	}

	@Override
	public Set<String> zrangeByScore(String key, double min, double max) {
		
		return getJedis().zrangeByScore(key, min, max);
	}

	@Override
	public Set<String> zrangeByScore(String key, String min, String max) {
		
		return getJedis().zrangeByScore(key, min, max);
	}

	@Override
	public Set<String> zrevrangeByScore(String key, double max, double min) {
		
		return getJedis().zrevrangeByScore(key, max, min);
	}

	@Override
	public Set<String> zrangeByScore(String key, double min, double max, int offset, int count) {
		
		return getJedis().zrangeByScore(key, min, max, offset, count);
	}

	@Override
	public Set<String> zrevrangeByScore(String key, String max, String min) {
		
		return getJedis().zrevrangeByScore(key, max, min);
	}

	@Override
	public Set<String> zrangeByScore(String key, String min, String max, int offset, int count) {
		
		return getJedis().zrangeByScore(key, max, min, offset, count);
	}

	@Override
	public Set<String> zrevrangeByScore(String key, double max, double min, int offset, int count) {
		
		return getJedis().zrevrangeByScore(key, max, min, offset, count);
	}

	@Override
	public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max) {
		
		return getJedis().zrangeByScoreWithScores(key, min, max);
	}

	@Override
	public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min) {
		
		return getJedis().zrevrangeByScoreWithScores(key, max, min);
	}

	@Override
	public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max, int offset, int count) {
		
		return getJedis().zrangeByScoreWithScores(key, min, max,offset,count);
	}

	@Override
	public Set<String> zrevrangeByScore(String key, String max, String min, int offset, int count) {
		
		return getJedis().zrevrangeByScore(key, max, min, offset, count);
	}

	@Override
	public Set<Tuple> zrangeByScoreWithScores(String key, String min, String max) {
		
		return getJedis().zrangeByScoreWithScores(key, min, max);
	}

	@Override
	public Set<Tuple> zrevrangeByScoreWithScores(String key, String max, String min) {
		
		return getJedis().zrevrangeByScoreWithScores(key, max, min);
	}

	@Override
	public Set<Tuple> zrangeByScoreWithScores(String key, String min, String max, int offset, int count) {
		
		return getJedis().zrangeByScoreWithScores(key, min, max,offset,count);
	}

	@Override
	public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min, int offset, int count) {
		
		return getJedis().zrevrangeByScoreWithScores(key, max, min, offset, count);
	}

	@Override
	public Set<Tuple> zrevrangeByScoreWithScores(String key, String max, String min, int offset, int count) {
		
		return getJedis().zrevrangeByScoreWithScores(key, max, min, offset, count);
	}

	@Override
	public Long zremrangeByRank(String key, long start, long end) {
		
		return getJedis().zremrangeByRank(key, start, end);
	}

	@Override
	public Long zremrangeByScore(String key, double start, double end) {
		
		return getJedis().zremrangeByScore(key, start, end);
	}

	@Override
	public Long zremrangeByScore(String key, String start, String end) {
		
		return getJedis().zremrangeByScore(key, start, end);
	}

	@Override
	public Long zlexcount(String key, String min, String max) {
		
		return getJedis().zlexcount(key, min, max);
	}

	@Override
	public Set<String> zrangeByLex(String key, String min, String max) {
		
		return getJedis().zrangeByLex(key, min, max);
	}

	@Override
	public Set<String> zrangeByLex(String key, String min, String max, int offset, int count) {
		
		return getJedis().zrangeByLex(key, min, max, offset,count);
	}

	@Override
	public Set<String> zrevrangeByLex(String key, String max, String min) {
		
		return getJedis().zrevrangeByLex(key, max, min);
	}

	@Override
	public Set<String> zrevrangeByLex(String key, String max, String min, int offset, int count) {
		
		return getJedis().zrevrangeByLex(key, min, max, offset, count);
	}

	@Override
	public Long zremrangeByLex(String key, String min, String max) {
		
		return getJedis().zremrangeByLex(key, min, max);
	}

	@Override
	public Long linsert(String key, LIST_POSITION where, String pivot, String value) {
		
		return getJedis().linsert(key, where, pivot, value);
	}

	@Override
	public Long lpushx(String key, String... string) {
		
		return getJedis().lpushx(key, string);
	}

	@Override
	public Long rpushx(String key, String... string) {
		
		return getJedis().rpushx(key, string);
	}

	@Override
	public List<String> blpop(String arg) {
		
		return getJedis().blpop(arg);
	}

	@Override
	public List<String> blpop(int timeout, String key) {
		
		return getJedis().blpop(timeout, key);
	}

	@Override
	public List<String> brpop(String arg) {
		
		return getJedis().brpop(arg);
	}

	@Override
	public List<String> brpop(int timeout, String key) {
		
		return getJedis().brpop(timeout, key);
	}

	@Override
	public Long del(String key) {
		
		return getJedis().del(key);
	}

	@Override
	public String echo(String string) {
		
		return getJedis().echo(string);
	}

	@Override
	public Long move(String key, int dbIndex) {
		
		return getJedis().move(key, dbIndex);
	}

	@Override
	public Long bitcount(String key) {
		
		return getJedis().bitcount(key);
	}

	@Override
	public Long bitcount(String key, long start, long end) {
		
		return getJedis().bitcount(key, start, end);
	}

	@Override
	public Long bitpos(String key, boolean value) {
		
		return getJedis().bitpos(key, value);
	}

	@Override
	public Long bitpos(String key, boolean value, BitPosParams params) {
		
		return getJedis().bitpos(key, value, params);
	}

	@Override
	public ScanResult<Entry<String, String>> hscan(String key, int cursor) {
		
		return getJedis().hscan(key, cursor);
	}

	@Override
	public ScanResult<String> sscan(String key, int cursor) {
		
		return getJedis().sscan(key, cursor);
	}

	@Override
	public ScanResult<Tuple> zscan(String key, int cursor) {
		
		return getJedis().zscan(key, cursor);
	}

	@Override
	public ScanResult<Entry<String, String>> hscan(String key, String cursor) {
		
		return getJedis().hscan(key, cursor);
	}

	@Override
	public ScanResult<Entry<String, String>> hscan(String key, String cursor, ScanParams params) {
		
		return getJedis().hscan(key, cursor, params);
	}

	@Override
	public ScanResult<String> sscan(String key, String cursor) {
		
		return getJedis().sscan(key, cursor);
	}

	@Override
	public ScanResult<String> sscan(String key, String cursor, ScanParams params) {
		
		return getJedis().sscan(key, cursor, params);
	}

	@Override
	public ScanResult<Tuple> zscan(String key, String cursor) {
		
		return getJedis().zscan(key, cursor);
	}

	@Override
	public ScanResult<Tuple> zscan(String key, String cursor, ScanParams params) {
		
		return getJedis().zscan(key, cursor, params);
	}

	@Override
	public Long pfadd(String key, String... elements) {
		
		return getJedis().pfadd(key, elements);
	}

	@Override
	public long pfcount(String key) {
		
		return getJedis().pfcount(key);
	}

	@Override
	public Long geoadd(String key, double longitude, double latitude, String member) {
		
		return getJedis().geoadd(key, longitude, latitude, member);
	}

	@Override
	public Long geoadd(String key, Map<String, GeoCoordinate> memberCoordinateMap) {
		
		return getJedis().geoadd(key, memberCoordinateMap);
	}

	@Override
	public Double geodist(String key, String member1, String member2) {
		
		return getJedis().geodist(key, member1, member2);
	}

	@Override
	public Double geodist(String key, String member1, String member2, GeoUnit unit) {
		
		return getJedis().geodist(key, member1, member2, unit);
	}

	@Override
	public List<String> geohash(String key, String... members) {
		
		return getJedis().geohash(key, members);
	}

	@Override
	public List<GeoCoordinate> geopos(String key, String... members) {
		
		return getJedis().geopos(key, members);
	}

	@Override
	public List<GeoRadiusResponse> georadius(String key, double longitude, double latitude, double radius,
			GeoUnit unit) {
		
		return getJedis().georadius(key, longitude, latitude, radius, unit);
	}

	@Override
	public List<GeoRadiusResponse> georadius(String key, double longitude, double latitude, double radius, GeoUnit unit,
			GeoRadiusParam param) {
		
		return getJedis().georadius(key, longitude, latitude, radius, unit, param);
	}

	@Override
	public List<GeoRadiusResponse> georadiusByMember(String key, String member, double radius, GeoUnit unit) {
		
		return getJedis().georadiusByMember(key, member, radius, unit);
	}

	@Override
	public List<GeoRadiusResponse> georadiusByMember(String key, String member, double radius, GeoUnit unit,
			GeoRadiusParam param) {
		
		return getJedis().georadiusByMember(key, member, radius, unit, param);
	}

	@Override
	public List<Long> bitfield(String key, String... arguments) {
		
		return getJedis().bitfield(key, arguments);
	}

}
