package vegoo.stockdata.db.gdhs;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.Dictionary;
import java.util.List;
import java.util.Map;

import org.osgi.service.cm.ConfigurationException;
import org.osgi.service.cm.ManagedService;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.EmptyResultDataAccessException;

import vegoo.jdbcservice.JdbcService;
import vegoo.redis.RedisService;
import vegoo.stockdata.db.GdhsPersistentService;
import vegoo.stockdata.db.HQPersistentService;
import vegoo.stockdata.db.base.PersistentServiceImpl;

@Component (immediate = true)
public class GdhsPersistentServiceImpl extends PersistentServiceImpl implements GdhsPersistentService, ManagedService{
	private static final Logger logger = LoggerFactory.getLogger(GdhsPersistentServiceImpl.class);

    @Reference private RedisService redis;

    @Reference private JdbcService db;
    
    @Reference  
    private HQPersistentService dbKDayData; // (policy=ReferencePolicy.DYNAMIC)

    @Override
	public void updated(Dictionary<String, ?> properties) throws ConfigurationException {
		
		
	}

    @Activate
    private void activate() {
		logger.info("OSGI BUNDLE:{} activated.", this.getClass().getName());
   	
    }
	
	private static final String SQL_EXIST_GDHS = "select stockcode from gdhs where stockcode=? and enddate=?";
	public boolean existGDHS(String stockCode, Date endDate) {
		try {
			String val = db.queryForObject(SQL_EXIST_GDHS, new Object[] {stockCode, endDate}, 
					new int[] {Types.VARCHAR, Types.DATE}, String.class);

			return  val != null;
		}catch(EmptyResultDataAccessException e) {
			return false;
		}catch(Exception e) {
    		logger.error(String.format("stockCode:%s, endDate:%tF%n", stockCode, endDate), e);
			return false;
		}
	}

	private static final String SQL_ADDGDHS = "insert into gdhs(stockCode, EndDate, "
			+"HolderNum,PreviousHolderNum,HolderNumChange,HolderNumChangeRate,RangeChangeRate, "
			+"PreviousEndDate,HolderAvgCapitalisation,HolderAvgStockQuantity,TotalCapitalisation, "
			+"CapitalStock,NoticeDate) "         // ,CapitalStockChange,CapitalStockChangeEvent,ClosePrice
			+"values (?,?,?,?,?,?,?,?,?,?,?,?,?)"; //,?,?,?
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
		// Date endTradeDate = dbKDayData.getLastTradeDate(stockCode, endDate);

		db.update( SQL_ADDGDHS, new Object[] {stockCode, endDate, 
				   holderNum,  previousHolderNum,
					 holderNumChange,  holderNumChangeRate,  rangeChangeRate,  previousEndDate,
					 holderAvgCapitalisation,  holderAvgStockQuantity,  totalCapitalisation,
					 capitalStock,  noticeDate}, ///*, item.getCapitalStockChange(), item.getCapitalStockChangeEvent(), item.getClosePrice()*/
				new int[] {Types.VARCHAR, Types.DATE,  Types.DOUBLE,Types.DOUBLE,Types.DOUBLE,
						   Types.DOUBLE, Types.DOUBLE, Types.DATE,  Types.DOUBLE, Types.DOUBLE, Types.DOUBLE,
						   Types.DOUBLE,Types.DATE});  ///*,Types.DOUBLE,Types.VARCHAR,Types.DOUBLE*/
	}

	@Override
	public void settleGdhs() {
		List<Date> reportDates = queryReportDateOfGdhsNullTransDate();
		for(Date rptDate:reportDates) {
			processGdhs(rptDate);
		}
	}
	
	private static final String QRY_NULL_STKS = "select distinct enddate from gdhs where EndTradeDate is null order by enddate desc";
	private List<Date> queryReportDateOfGdhsNullTransDate() {
		try {
			return db.queryForList(QRY_NULL_STKS, Date.class);
		}catch(Exception e) {
			logger.error("", e);
			return new ArrayList<>();
		}
	}

	private int processGdhs(Date rptDate) {
		List<Map<String, Object>> items = queryNullTransDateOfGDHS(rptDate);
		if(items==null||items.isEmpty()) {
			return 0;
		}
		for(Map<String, Object> item:items) {
		  updateNullTransDateOfGDHS(rptDate, item);
		}
		return items.size();
	}
	
	private static final String QRY_GDHS_NULLTRNS="select id, stockcode from gdhs where enddate=? and EndTradeDate is null";
    private List<Map<String, Object>> queryNullTransDateOfGDHS(Date rptDate){
    	try {
    		return db.queryForList(QRY_GDHS_NULLTRNS, new Object[] {rptDate}, 
    				new int[] {Types.DATE});
    	}catch(Exception e) {
    		logger.error("",e);
    		return null;
    	}
    }
	
	private void updateNullTransDateOfGDHS(Date enddate,Map<String, Object> item) {
		String stockCode = (String) item.get("stockcode");
		Integer id = (Integer) item.get("id");

		Date endTradeDate = dbKDayData.getLastTradeDate(stockCode, enddate);
		if(endTradeDate==null) {
			return;
		}
		
		updateNullTransDateOfGDHS(id, endTradeDate);
	}
	
	private static final String UPD_GDHS_TRNSDATE="update gdhs set EndTradeDate=? where id=?";
	private void updateNullTransDateOfGDHS(Integer id, Date endTradeDate) {
		try {
			db.update( UPD_GDHS_TRNSDATE, new Object[] {endTradeDate, id},
					new int[] {Types.DATE, Types.INTEGER});
		}catch(EmptyResultDataAccessException e) {
			return;
		}catch(Exception e) {
			logger.error("",e);
			return;
		}
	}
	
}
