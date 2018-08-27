package vegoo.stockdata.db.block;

import java.sql.Types;
import java.util.Dictionary;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.osgi.service.cm.ConfigurationException;
import org.osgi.service.cm.ManagedService;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.EmptyResultDataAccessException;

import com.google.common.collect.Sets;

import vegoo.jdbcservice.JdbcService;
import vegoo.redis.RedisService;
import vegoo.stockdata.db.BlockPersistentService;
import vegoo.stockdata.db.base.PersistentServiceImpl;
import vegoo.stockdata.db.base.Redis;

@Component (immediate = true)
public class BlockPersistentServiceImpl extends PersistentServiceImpl implements BlockPersistentService, ManagedService {
	private static final Logger logger = LoggerFactory.getLogger(BlockPersistentServiceImpl.class);

    @Reference private RedisService redis;

    @Reference private JdbcService db;
	
	@Override
	public void updated(Dictionary<String, ?> properties) throws ConfigurationException {
		logger.info("{} configured.", this.getClass().getName());
	}
	
    @Activate
    private void activate() {
		logger.info("OSGI BUNDLE:{} activated.", this.getClass().getName());

		//if(redis.hget(Redis.KEY_CACHED_TBLS, Redis.TABLENAME_BLOCK)==null) {
		//   db.execute("call block_INIT_REDIS()");	// 初始化缓存
		//}
    }
	
   @Override
    public boolean existBlock(String blockUCode) {
    	return existBlockByDB(blockUCode);
    }
    
/*    private boolean existBlockByRedis(String blockUCode) {
    	return redis.sismember(Redis.KEY_BLOCKS, blockUCode);
    }
*/
	private static String SQL_EXIST_BLKINFO = "select blkUCode from block where blkucode=?";
	private boolean existBlockByDB(String blockUCode) {
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
    	   db.update( SQL_NEW_BLKINFO, 
    			   new Object[] {marketid, blkCode, blkname, blkUCode, blkType}, 
    			   new int[]{Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,Types.VARCHAR});
    	}catch(Exception e) {
    		logger.error("插入block记录出错：{}.{}.{}.{}/{}", marketid, blkCode, blkname, blkUCode, blkType);
    	}
    }
    
	private static String SQL_DELALL_STKOFBLK = "delete from stocksOfBlock where blkUcode=?";
    @Override
    public void deleteAllStocksOfBlock(String blkUcode) {
    	try {
     	   db.update( SQL_DELALL_STKOFBLK, 
     			   new Object[] { blkUcode},
     			   new int[] {Types.VARCHAR});
     	}catch(Exception e) {
     		logger.error("",e);
     	}
	}
    
	private static String SQL_INS_STKOFBLK = "insert into stocksOfBlock(blkUcode,stockCode) values(?,?)";
    @Override
    public void insertStockOfBlock(String blkUcode, String stkCode) {
    	try {
     	   db.update( SQL_INS_STKOFBLK, 
     			   new Object[] { blkUcode, stkCode}, 
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
     	   db.update( SQL_DEL_STKOFBLK, 
     			   new Object[] { blkUcode, stkCode}, 
     			   new int[] {Types.VARCHAR, Types.VARCHAR});
     	}catch(Exception e) {
     		logger.error("插入StockOfBlock是出错：blkUcode:{}, stkCode:{}, marketid:{}",blkUcode, stkCode);
     		logger.error("", e);
     	}
	}
    
	@Override
	public void updateStocksOfBlock(String blkUcode, Set<String> newMembers) {
		// String key = Redis.keyStocksOfBlock(blkUcode);
		// Set<String> oldMembers = redis.smembers(key);
		Set<String> oldMembers = getStockCodesOfBlock(blkUcode);
		Set<String> addMembers = Sets.difference(newMembers, oldMembers); // 差集 1中有而2中没有的
		Set<String> DelMembers = Sets.difference(oldMembers, newMembers); // 差集 1中有而2中没有的
		
		for(String stkCode : DelMembers) {
			deleteStockOfBlock(blkUcode, stkCode);
		}
		
		for(String stkCode : addMembers) {
			insertStockOfBlock(blkUcode, stkCode);
		}
	}
	
	private static final String SQL_SEL_STKC_BLK="select stockCode from stocksOfBlock where blkucode=?";
	private Set<String> getStockCodesOfBlock(String blkUcode){
		Set<String> result = new HashSet<>();
		try {
			List<String> stockCodes = db.queryForList(SQL_SEL_STKC_BLK, 
					new Object[] {blkUcode}, 
					new int[] {Types.VARCHAR}, 
					String.class);
			result.addAll(stockCodes);
		}catch(Exception e) {
			logger.error("",e);
		}
		return result;
	}


}
