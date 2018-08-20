package vegoo.stockdata.db.impl;

import java.sql.Types;
import java.util.Dictionary;
import java.util.Set;

import org.osgi.service.cm.ConfigurationException;
import org.osgi.service.cm.ManagedService;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.EmptyResultDataAccessException;

import com.google.common.collect.Sets;

import vegoo.jdbcservice.JdbcService;
import vegoo.redis.RedisService;
import vegoo.stockdata.db.BlockPersistentService;
import vegoo.stockdata.db.PersistentService;
import vegoo.stockdata.db.RedisKey;

@Component (
		immediate = true, 
		//configurationPid = "stockdata.persistent",
		//service = { PersistentService.class,  ManagedService.class}, 
		property = {
		    //Scheduler.PROPERTY_SCHEDULER_EXPRESSION + "= 0 0 1,18 * * ?", //  静态信息，每天7，8，18抓三次
		    // Scheduler.PROPERTY_SCHEDULER_CONCURRENT + "= false"
		} 
	)
public class BlockPersistentServiceImpl extends PersistentServiceImpl implements BlockPersistentService, ManagedService {
	private static final Logger logger = LoggerFactory.getLogger(BlockPersistentServiceImpl.class);

    @Reference
    volatile private RedisService redis;

    @Reference
    private JdbcService db;
	
	@Override
	public void updated(Dictionary<String, ?> properties) throws ConfigurationException {
		logger.info("{} configured.", this.getClass().getName());
		
	}
	
	private static String SQL_EXIST_BLKINFO = "select blkUCode from block where blkucode=?";
    @Override
    public boolean existBlock(String blockUCode) {
    	return existBlockByRedis(blockUCode);
    }
    
    public boolean existBlockByRedis(String blockUCode) {
    	return redis.sismember(RedisKey.Blocks(), blockUCode);
    }

    public boolean existBlockByDB(String blockUCode) {
    	try {
    		String val = db.queryForObject(SQL_EXIST_BLKINFO, new Object[] {blockUCode}, new int[] {Types.VARCHAR}, String.class);
		    return  val != null;
		}catch(EmptyResultDataAccessException e) {
			return false;
    	}catch(Exception e) {
    		logger.error("",e);
    		return false;
    	}
    }

	private static String SQL_NEW_BLKINFO = "insert into block(marketid,blkcode,blkname,blkucode,blktype) values (?,?,?,?,?)";
    @Override
	public void insertBlock(String blkType, String marketid, String blkCode, String blkname, String blkUCode) {
    	try {
    	   db.update(SQL_NEW_BLKINFO, new Object[] {marketid, blkCode, blkname, blkUCode, blkType}, 
    			   new int[]{Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,Types.VARCHAR});
    	}catch(Exception e) {
    		logger.error("插入block记录出错：{}.{}.{}.{}/{}", marketid, blkCode, blkname, blkUCode, blkType);
    	}
    }
    
	private static String SQL_DELALL_STKOFBLK = "delete from stocksOfBlock where blkUcode=?";
    @Override
    public void deleteAllStocksOfBlock(String blkUcode) {
    	try {
     	   db.update(SQL_DELALL_STKOFBLK, new Object[] { blkUcode}, new int[] {Types.VARCHAR});
     	}catch(Exception e) {
     		logger.error("",e);
     	}
	}
    
	private static String SQL_INS_STKOFBLK = "insert into stocksOfBlock(blkUcode,stockCode,marketid) values(?,?,?)";
    @Override
    public void insertStockOfBlock(String blkUcode, String stkCode) {
    	try {
     	   db.update(SQL_INS_STKOFBLK, new Object[] { blkUcode, stkCode}, 
     			   new int[] {Types.VARCHAR, Types.VARCHAR});
     	}catch(Exception e) {
     		logger.error("插入StockOfBlock是出错：blkUcode:{}, stkCode:{}, marketid:{}",blkUcode, stkCode);
     		logger.error("", e);
     	}
	}

	private static String SQL_DEL_STKOFBLK = "delete from stocksOfBlock where blkUcode=? and stockCode=?";
    @Override
    public void deleteStockOfBlock(String blkUcode, String stkCode) {
    	try {
     	   db.update(SQL_DEL_STKOFBLK, new Object[] { blkUcode, stkCode}, 
     			   new int[] {Types.VARCHAR, Types.VARCHAR});
     	}catch(Exception e) {
     		logger.error("插入StockOfBlock是出错：blkUcode:{}, stkCode:{}, marketid:{}",blkUcode, stkCode);
     		logger.error("", e);
     	}
	}
    
	@Override
	public void updateStocksOfBlock(String blkUcode, Set<String> newMembers) {
		String key = RedisKey.StocksOfBlock(blkUcode);
		Set<String> oldMembers = redis.smembers(key);
		
		Set<String> addMembers = Sets.difference(newMembers, oldMembers); // 差集 1中有而2中没有的
		Set<String> DelMembers = Sets.difference(oldMembers, newMembers); // 差集 1中有而2中没有的
		
		for(String stkCode : DelMembers) {
			deleteStockOfBlock(blkUcode, stkCode);
		}
		
		for(String stkCode : addMembers) {
			insertStockOfBlock(blkUcode, stkCode);
		}
	}


}
