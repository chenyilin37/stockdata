package vegoo.stockdata.db.hq;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Date;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.osgi.service.cm.ConfigurationException;
import org.osgi.service.cm.ManagedService;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;

import vegoo.jdbcservice.JdbcService;
import vegoo.redis.RedisService;
import vegoo.stockdata.core.model.KDayDao;
import vegoo.stockdata.core.utils.StockUtil;
import vegoo.stockdata.db.HQPersistentService;
import vegoo.stockdata.db.base.PersistentServiceImpl;

@Component (immediate = true)
public class HQPersistentServiceImpl extends PersistentServiceImpl implements HQPersistentService, ManagedService{
	private static final Logger logger = LoggerFactory.getLogger(HQPersistentServiceImpl.class);

    @Reference private RedisService redis;

    @Reference private JdbcService db;

	@Override
	public void updated(Dictionary<String, ?> properties) throws ConfigurationException {
		/* ！！！本函数内不要做需要长时间才能完成的工作，否则，会影响其他BUNDLE的初始化！！！  */
		
	}

	/*
	 * K线数据截止日期
	 */
	@Override
	public Date getLastTradeDate(String stockCode) {
		return getLastTradeDate(stockCode, StockUtil.getLastTransDate(true));
	}
	
	/* 因为enddate有可能不是交易日，或者该股票当日停牌不交易，
	 * 求季报日期endDate对应的在最后一个交易日，以便在指标图上显示
	 */
	private static final Map<String,Date> stockTransDateCache = new HashMap<>();
	@Override
	public Date getLastTradeDate(String stockCode, Date endDate) {
		String key = String.format("%s-%tF", stockCode, endDate);
		Date result = stockTransDateCache.get(key);
		if(result == null) {
			result = queryLastTradeDate(stockCode, endDate);
			if(result != null) {
			   stockTransDateCache.put(key, result);
			}
		}
		return result;
	}
	
	private static final String QRY_LAST_TRNSDATE="select transDate from kdaydata where scode=? and transDate<=? order by transDate desc limit 1";
	private Date queryLastTradeDate(String stockCode, Date endDate) {
		try {
			return db.queryForObject(QRY_LAST_TRNSDATE, 
					new Object[] {stockCode, endDate},
					new int[] {Types.VARCHAR, Types.DATE}, 
					Date.class);
		}catch(EmptyResultDataAccessException e) {
			return queryLastTradeDateForNewStock(stockCode);
		}catch(Exception e) {
			logger.error("",e);
			return null;
		}
	}
	
	/**
	 * 发布公告时，股票还没有上市的新股,
	 */
	private static final String QRY_LAST_TRNSDATE_NEW="select transDate from kdaydata where scode=? order by transDate limit 1";
	private Date queryLastTradeDateForNewStock(String stockCode) {
		try {
			return db.queryForObject(QRY_LAST_TRNSDATE_NEW, 
					new Object[] {stockCode},
					new int[] {Types.VARCHAR}, 
					Date.class);
		}catch(EmptyResultDataAccessException e) {
			return null;
		}catch(Exception e) {
			logger.error("",e);
			return null;
		}
	}

	private static final String SQL_BUDP_KDAY = "insert into kdaydata(SCode,transDate,open,close,high,low,volume,amount,amplitude,turnoverRate,changeRate,LClose) values(?,?,?,?,?,?,?,?,?,?,?,?)";
	@Override
	public void saveKDayData(List<KDayDao> newItems) {
		db.batchUpdate( SQL_BUDP_KDAY, 
				new BatchPreparedStatementSetter(){

			@Override
			public int getBatchSize() {
				return newItems.size();
			}

			@Override
			public void setValues(PreparedStatement ps, int i) throws SQLException {
				KDayDao item = newItems.get(i);
				
				ps.setString(1, item.getSCode());
				ps.setDate(2, new java.sql.Date(item.getTransDate().getTime()));
				ps.setDouble(3, item.getOpen());
				ps.setDouble(4, item.getClose());
				ps.setDouble(5, item.getHigh());
				ps.setDouble(6, item.getLow());
				ps.setDouble(7, item.getVolume());
				ps.setDouble(8, item.getAmount());
				ps.setDouble(9, item.getAmplitude());
				ps.setDouble(10, item.getTurnoverRate());
				ps.setDouble(11, item.getChangeRate());
				ps.setDouble(12, item.getLClose());
			}});		
	}

	private static final String QRY_CLOSEPRICE="select close from kdaydata where SCode=? and transDate=?";
	@Override
	public double getClosePrice(String sCode, Date transDate) {
		try {
			return db.queryForObject(QRY_CLOSEPRICE, 
						new Object[] {sCode, transDate},
						new int[] {Types.VARCHAR, Types.DATE},
						Double.class);
		}catch(Exception e) {
			return 0;
		}
	}
	
	
}
