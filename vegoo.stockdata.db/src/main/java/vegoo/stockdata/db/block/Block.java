package vegoo.stockdata.db.block;

import vegoo.redis.RedisService;

public class Block {
	private static final String KEY_BLOCK = "block_%s";
	
	private RedisService redis;
	private final String blkucode;
	
	public Block(String blkucode, RedisService redis) {
		this.blkucode = blkucode;
		this.redis = redis;
	}
	
	private String getKey() {
		return String.format(KEY_BLOCK, blkucode);
	}

	public String getBlkucode() {
		return blkucode;
	}

	public String getBlkcode() {
		return redis.hget(getKey(), "blkcode");
	}
	public void setBlkcode(String blkcode) {
		redis.hset(getKey(), "blkcode", blkcode);
	}
	public String getMarketid() {
		return redis.hget(getKey(), "marketid");
	}
	public void setMarketid(String marketid) {
		redis.hset(getKey(), "marketid", marketid);
	}
	public String getBlkname() {
		return redis.hget(getKey(), "blkname");
	}
	public void setBlkname(String blkname) {
		redis.hset(getKey(), "blkname", blkname);
	}
	public String getBlktype() {
		return redis.hget(getKey(), "blktype");
	}
	public void setBlktype(String blktype) {
		redis.hset(getKey(), "blktype", blktype);
	}

	public String toString() {
		StringBuffer sb = new  StringBuffer();
		
		sb.append(String.format("blkucode:%s ", getBlkucode()) );
		sb.append(String.format("blkcode:%s ", getBlkcode()) );
		sb.append(String.format("blkname:%s ", getBlkname()) );
		sb.append(String.format("blktype:%s ", getBlktype()) );
		
		
		return sb.toString();
	}
	
	
}
