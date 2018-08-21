package vegoo.stockdata.db.gdhs;

import java.sql.Types;
import java.util.Date;
import java.util.Dictionary;

import org.osgi.service.cm.ConfigurationException;
import org.osgi.service.cm.ManagedService;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.EmptyResultDataAccessException;

import vegoo.jdbcservice.JdbcService;
import vegoo.redis.RedisService;
import vegoo.stockdata.db.GdhsPersistentService;
import vegoo.stockdata.db.base.PersistentServiceImpl;

@Component (immediate = true)
public class GdhsPersistentServiceImpl extends PersistentServiceImpl implements GdhsPersistentService, ManagedService{
	private static final Logger logger = LoggerFactory.getLogger(GdhsPersistentServiceImpl.class);

    @Reference private RedisService redis;

    @Reference private JdbcService db;

    @Override
	public void updated(Dictionary<String, ?> properties) throws ConfigurationException {
		// TODO Auto-generated method stub
		
	}

	
	private static final String SQL_EXIST_GDHS = "select stockcode from gdhs where stockcode=? and enddate=?";
	public boolean existGDHS(String stockCode, Date endDate) {
		try {
			String val = db.queryForObject(SQL_EXIST_GDHS, new Object[] {stockCode, endDate}, 
					new int[] {Types.VARCHAR, Types.DATE}, String.class);
		    // logger.info("vvvvv={}", val);
			return  val != null;
		}catch(EmptyResultDataAccessException e) {
			return false;
		}catch(Exception e) {
    		logger.error(String.format("stockCode:%s, endDate:%tF%n", stockCode, endDate), e);
			return false;
		}
	}

	private static final String SQL_ADDGDHS = "insert into gdhs(stockCode, EndDate, EndTradeDate, "
			+"HolderNum,PreviousHolderNum,HolderNumChange,HolderNumChangeRate,RangeChangeRate, "
			+"PreviousEndDate,HolderAvgCapitalisation,HolderAvgStockQuantity,TotalCapitalisation, "
			+"CapitalStock,NoticeDate) "         // ,CapitalStockChange,CapitalStockChangeEvent,ClosePrice
			+"values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)"; //,?,?,?
	public void insertGdhs(String stockCode, Date endDate, double holderNum, double previousHolderNum,
			double holderNumChange, double holderNumChangeRate, double rangeChangeRate, Date previousEndDate,
			double holderAvgCapitalisation, double holderAvgStockQuantity, double totalCapitalisation,
			double capitalStock, Date noticeDate) {
		
		if(endDate == null) {
			return;
		}
		// 如果股东户数为0，可能是数据错误，抛弃
		if(holderNum<1) {
			return;
		}
		
		// 因为enddate有可能不是交易日，或者该股票当日停牌不交易，
		// 求季报日期endDate对应的在最后一个交易日，以便在指标图上显示
		Date endTradeDate = getLastTradeDate(endDate, stockCode, db);

		db.update(SQL_ADDGDHS, new Object[] {stockCode, endDate, endTradeDate,
				   holderNum,  previousHolderNum,
					 holderNumChange,  holderNumChangeRate,  rangeChangeRate,  previousEndDate,
					 holderAvgCapitalisation,  holderAvgStockQuantity,  totalCapitalisation,
					 capitalStock,  noticeDate}, ///*, item.getCapitalStockChange(), item.getCapitalStockChangeEvent(), item.getClosePrice()*/
				new int[] {Types.VARCHAR, Types.DATE, Types.DATE, Types.DOUBLE,Types.DOUBLE,Types.DOUBLE,
						   Types.DOUBLE, Types.DOUBLE, Types.DATE,  Types.DOUBLE, Types.DOUBLE, Types.DOUBLE,
						   Types.DOUBLE,Types.DATE});  ///*,Types.DOUBLE,Types.VARCHAR,Types.DOUBLE*/
	}

	/* 因为enddate有可能不是交易日，或者该股票当日停牌不交易，
	 * 求季报日期endDate对应的在最后一个交易日，以便在指标图上显示
	 * 
	 */
	private static final String QRY_LAST_TRNSDATE="select transDate from kdaydata where scode=? and transDate<=?  order by transDate desc limit 1";
	private Date getLastTradeDate(Date endDate, String stockCode, JdbcService db) {
		try {
			return db.queryForObject(QRY_LAST_TRNSDATE, new Object[] {stockCode, endDate},
					new int[] {Types.VARCHAR, Types.DATE}, Date.class);
		}catch(EmptyResultDataAccessException e) {
			return null;
		}catch(Exception e) {
			logger.error("",e);
			return null;
		}
	}
	
}
