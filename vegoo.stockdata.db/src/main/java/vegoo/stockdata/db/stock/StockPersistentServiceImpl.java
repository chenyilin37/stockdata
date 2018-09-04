package vegoo.stockdata.db.stock;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.Dictionary;
import java.util.List;

import org.osgi.service.cm.ConfigurationException;
import org.osgi.service.cm.ManagedService;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.EmptyResultDataAccessException;

import vegoo.jdbcservice.JdbcService;
import vegoo.redis.RedisService;
import vegoo.stockdata.core.model.StockCapitalDao;
import vegoo.stockdata.db.StockPersistentService;
import vegoo.stockdata.db.base.PersistentServiceImpl;
import vegoo.stockdata.db.gudong.GudongPersistentServiceImpl;


@Component (immediate = true)
public class StockPersistentServiceImpl extends PersistentServiceImpl implements StockPersistentService, ManagedService {
	private static final Logger logger = LoggerFactory.getLogger(GudongPersistentServiceImpl.class);

    @Reference private RedisService redis;

    @Reference private JdbcService db;
	
	@Override
	public void updated(Dictionary<String, ?> properties) throws ConfigurationException {
		
	}
	
	private static final String QRY_STKS = "SELECT stockCode FROM stock";
	@Override
	public List<String> getAllStockCodes() {
		try {
			return db.queryForList(QRY_STKS, String.class);
		}catch(Exception e) {
			logger.error("", e);
			return new ArrayList<>();
		}
	}
	
    private static String SQL_EXIST_STK = "select 1 from stock where stockCode=?";
    public boolean existStock(String stkCode) {
    	try {
    		Integer val = db.queryForObject(SQL_EXIST_STK, new Object[] {stkCode}, new int[] {Types.VARCHAR}, Integer.class);
    		return  val !=null;
		}catch(EmptyResultDataAccessException e) {
			return false;
    	}catch(Exception e) {
    		logger.error("", e);
    		return false;
    	}
    }

	private static String SQL_NEW_STK = "insert into stock(marketid,stockCode,stockName,stockUcode,isNew) values (?,?,?,?,?)";
	public void insertStock(String marketid, String stkCode, String stkName, String stkUCode) {
    	boolean isNew = stkName.startsWith("N");
    	try {
    	   db.update( SQL_NEW_STK, new Object[] {marketid, stkCode, stkName, stkUCode, (isNew?1:0)}, 
    			   new int[]{Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,Types.INTEGER});
    	}catch(Exception e) {
    		logger.error("插入stock记录出错：{}.{}.{}.{}/{}", marketid, stkCode, stkName, stkUCode, isNew);
    		logger.error("", e);
    	}
    }

	private static final String QRY_EXIST_STKFLW="select 1 from stockCapital where stockCode=? and rdate=?";
	@Override
	public boolean existStockCapital(String stkcode, Date rDate) {
    	try {
    		Integer val = db.queryForObject(QRY_EXIST_STKFLW, new Object[] {stkcode, rDate},
    				new int[] {Types.VARCHAR,Types.DATE}, Integer.class);
		    return  val != null;
		}catch(EmptyResultDataAccessException e) {
			return false;
    	}catch(Exception e) {
    		logger.error("", e);
    		return false;
    	}
	}

	private static final String UPD_STKFLW="insert into stockCapital(stockCode,rdate,ltg) values(?,?,?)";
	@Override
	public void insertStockCapital(StockCapitalDao dao) {
		try {
			db.update( UPD_STKFLW, new Object[] {dao.getStockCode(), dao.getTransDate(), dao.getLtg()},
					              new int[] {Types.VARCHAR,Types.DATE, Types.DOUBLE});
		}catch(Exception e) {
    		logger.error("", e);
		}
		
	}
	
}
