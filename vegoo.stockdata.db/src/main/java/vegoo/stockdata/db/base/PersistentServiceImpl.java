package vegoo.stockdata.db.base;

import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;

import vegoo.commons.MyThreadPoolExecutor;
import vegoo.jdbcservice.JdbcService;

public abstract class PersistentServiceImpl{
	private static final Logger logger = LoggerFactory.getLogger(PersistentServiceImpl.class);
    // 核心线程数量
    private static int corePoolSize = 1;
    // 最大线程数量
    private static int maxPoolSize = 1;
    // 线程存活时间：当线程数量超过corePoolSize时，10秒钟空闲即关闭线程
    private static int keepAliveTime = 10*1000;
    // 缓冲队列
    // private static BlockingQueue<Runnable> workQueue = null;
    // 线程池
    private  ThreadPoolExecutor threadPoolExecutor =  new MyThreadPoolExecutor(corePoolSize, maxPoolSize, keepAliveTime, TimeUnit.SECONDS);
    
    public Future<?> submit(Runnable runnable) {
		return threadPoolExecutor.submit(runnable);
	}
    
    protected Future<?> update(JdbcService db,String sql, Object[] args, int[] argTypes) throws DataAccessException{
		return submit(new Runnable() {

			@Override
			public void run() {
				try {
				   db.update(sql, args, argTypes);
				}catch(Exception e) {
					logger.error("",e);
				}
			}});
	}

    protected Future<?> batchUpdate(JdbcService db, String sql,
			BatchPreparedStatementSetter batchPreparedStatementSetter) {
		return submit(new Runnable() {

			@Override
			public void run() {
				try {
					db.batchUpdate(sql, batchPreparedStatementSetter);
				}catch(Exception e) {
					logger.error("",e);
				}
			}});
	}
    
    
}
