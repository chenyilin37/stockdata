package vegoo.stockdata.db.block;

import java.sql.Types;
import java.util.Dictionary;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.karaf.scheduler.Job;
import org.osgi.service.cm.ConfigurationException;
import org.osgi.service.cm.ManagedService;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.EmptyResultDataAccessException;

import com.google.common.base.Strings;
import com.google.common.collect.Sets;

import vegoo.jdbcservice.JdbcService;
import vegoo.redis.RedisService;
import vegoo.stockdata.db.BlockPersistentService;
import vegoo.stockdata.db.base.PersistentServiceImpl;
import vegoo.stockdata.db.base.Redis;

@Component (
	immediate = true,
	service = { BlockPersistentService.class,  ManagedService.class},			
	configurationPid = "stockdata.db.block"
)
public class BlockPersistentServiceImpl extends PersistentServiceImpl implements BlockPersistentService, ManagedService {
	private static final Logger logger = LoggerFactory.getLogger(BlockPersistentServiceImpl.class);

    @Reference private RedisService redis;
    @Reference private JdbcService db;
    
    private int _cacheLevel_Block = CACHE_LEVEL_NONE;
    private int _cacheLevel_StkBlk = CACHE_LEVEL_NONE;
	
    @Activate
    private void activate() {
		/* ！！！本函数内不要做需要长时间才能完成的工作，否则，会影响其他BUNDLE的初始化！！！  */

    	logger.info("OSGI BUNDLE:{} activated, cacheLevel={}", this.getClass().getName(), _cacheLevel_Block);
    }

    @Override
	public void updated(Dictionary<String, ?> properties) throws ConfigurationException {
		/* ！！！本函数内不要做需要长时间才能完成的工作，否则，会影响其他BUNDLE的初始化！！！  */
		
    	String v = (String) properties.get(PN_CACHE_LEVEL);
		logger.info("{} configured .{}:{}", this.getClass().getName(), PN_CACHE_LEVEL, v);
		
    	if(!Strings.isNullOrEmpty(v)) {
	    	try {
	    		_cacheLevel_Block= Integer.parseInt(v.trim());
	    	}catch(Exception e) {
	    		_cacheLevel_Block = CACHE_LEVEL_NONE;
	    		logger.error("",e);
	    	}
    	}
		
    	// setCacheLevelBlock(_cacheLevel_Block);
		
/*		if(_cacheLevel_Block>CACHE_LEVEL_NONE) {
			_cacheLevel_StkBlk = CACHE_LEVEL_NDX;
			
			setCacheLevelStockOfBlock(_cacheLevel_StkBlk);
			
			asyncExecute(new Runnable() {

				@Override
				public void run() {
					initRedisCache();
				}});
		}
*/
	}
	
	
   private boolean block_cached = false;
   private boolean blkstk_cached = false;
   private void initRedisCache() {
		try {
		    db.update("call block_INIT_REDIS(?)", 
		    		   new Object[] {_cacheLevel_Block},
		    		   new int[] {Types.INTEGER});	// 初始化缓存
		    block_cached = true;
		}catch(Exception e) {
			setCacheLevelBlock(CACHE_LEVEL_NONE);
			logger.error("",e);
		}
		
		try {
		    db.update("call stocksOfBlock_INIT_REDIS(?)", 
		    		   new Object[] {_cacheLevel_StkBlk},
		    		   new int[] {Types.INTEGER});	// 初始化缓存
		    blkstk_cached = true;
		}catch(Exception e) {
			setCacheLevelStockOfBlock(CACHE_LEVEL_NONE);
			logger.error("",e);
		}
   }

   protected void setCacheLevelBlock(int cacheLevel) {
       this._cacheLevel_Block = cacheLevel;
	   super.setCacheLevel(redis, TBL_BLOCK, cacheLevel);
	}
   
   protected int getCacheLevelBlock() {
	   return block_cached ? _cacheLevel_Block : CACHE_LEVEL_NONE;
   }

   protected void setCacheLevelStockOfBlock(int cacheLevel) {
       this._cacheLevel_StkBlk = cacheLevel;
	   super.setCacheLevel(redis, TBL_STOCKOFBLOCK, cacheLevel);
	}
   
   protected int getCacheLevelStockOfBlock() {
	   return blkstk_cached ? _cacheLevel_StkBlk : CACHE_LEVEL_NONE;
   }
   
    @Override
    public boolean existBlock(String blockUCode) {
	   if(getCacheLevelBlock()>CACHE_LEVEL_NONE) {
		  return existBlockByRedis(blockUCode);
	   }else {
    	  return existBlockByDB(blockUCode);
	   }
    }

    private boolean existBlockByRedis(String blockUCode) {
    	return redis.sismember(Redis.KEY_BLOCKS, blockUCode);
    }

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
    		logger.error("",e);
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
     		logger.error("删除StockOfBlock是出错：blkUcode:{}, stkCode:{}, marketid:{}",blkUcode, stkCode);
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
	
	@Override
	public Set<String> getStockCodesOfBlock(String blkUcode){
		if(getCacheLevelStockOfBlock()>CACHE_LEVEL_NONE) {
			return getStocksOfBlockByRedis(blkUcode);
		}else {
			return getStocksOfBlockByDB(blkUcode);
		}
	}
	
	private Set<String> getStocksOfBlockByRedis(String blkUcode) {
		String key = String.format("STKBLK_%s",blkUcode);
		return redis.smembers(key);
	}

	private static final String SQL_SEL_STKC_BLK="select stockCode from stocksOfBlock where blkucode=?";
	private Set<String> getStocksOfBlockByDB(String blkUcode){
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
	
	@Override
	public Set<String> getBlocksOfStock(String stockcode) {
		if(getCacheLevelStockOfBlock()>CACHE_LEVEL_NONE) {
			return getBlocksOfStockByRedis(stockcode);
		}else {
			return getBlocksOfStockByDB(stockcode);
		}
	}

	private Set<String> getBlocksOfStockByRedis(String stockcode) {
		String key = String.format("BLKSTK_%s",stockcode);
		return redis.smembers(key);
	}
	
	private static final String QRY_BLKS_STK="select blkucode from stocksOfBlock where stockCode=?";
	private Set<String> getBlocksOfStockByDB(String stockcode){
		Set<String> result = new HashSet<>();
		try {
			List<String> blockCodes = db.queryForList(QRY_BLKS_STK, 
					new Object[] {stockcode}, 
					new int[] {Types.VARCHAR}, 
					String.class);
			result.addAll(blockCodes);
		}catch(Exception e) {
			logger.error("",e);
		}
		return result;
	}


}
