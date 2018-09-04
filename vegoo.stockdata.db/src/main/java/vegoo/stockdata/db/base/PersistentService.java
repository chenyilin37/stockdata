package vegoo.stockdata.db.base;

public interface PersistentService {
	// Global Redis Cache Keys
	String KEY_BLOCKS = "blocks";
	
	String KEY_CACHED_TABLES = "CACHED_TABLES";
	String PN_CACHE_LEVEL    = "cache-level";
	int CACHE_LEVEL_NONE   = 0;   // 未缓存
	int CACHE_LEVEL_NDX    = 1;   // 缓存了索引
	int CACHE_LEVEL_FULL   = 2;   // 缓存了索引和数据；
	

}
