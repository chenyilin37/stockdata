package vegoo.stockdata.db.base;

import java.util.Dictionary;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;

import vegoo.commons.MyThreadPoolExecutor;
import vegoo.jdbcservice.JdbcService;
import vegoo.redis.RedisService;

public abstract class PersistentServiceImpl implements PersistentService{
	private static final Logger logger = LoggerFactory.getLogger(PersistentServiceImpl.class);

	
	// 核心线程数量
    private static int corePoolSize = 1;
    // 最大线程数量
    private static int maxPoolSize = 10;
    // 线程存活时间：当线程数量超过corePoolSize时，10秒钟空闲即关闭线程
    private static int keepAliveTime = 10*1000;
    // 缓冲队列
    // private static BlockingQueue<Runnable> workQueue = null;
    // 线程池
    private static ThreadPoolExecutor threadPoolExecutor =  new MyThreadPoolExecutor(corePoolSize, maxPoolSize, keepAliveTime, TimeUnit.SECONDS);
    
    public static Future<?> asyncExecute(Runnable runnable) {
		return threadPoolExecutor.submit(runnable);
	}
    
    static protected void setCacheLevel(RedisService redis,String tableName, int cacheLevel) {
		redis.hset(KEY_CACHED_TABLES, tableName.toUpperCase(), String.valueOf(cacheLevel));
	}
	
    static protected int getCacheLevel(RedisService redis,String tableName) {
    	try {
    		String val = redis.hget(KEY_CACHED_TABLES, tableName.toUpperCase());
    		return Integer.parseInt(val);
		}catch(Exception e) {
			return 0;
		}
	}
    
}
