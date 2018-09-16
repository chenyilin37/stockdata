package vegoo.stockdata.db.gdhs;

import java.math.BigDecimal;
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

import vegoo.commons.jdbc.JdbcService;
import vegoo.commons.redis.RedisService;
import vegoo.stockcommon.utils.StockUtil;
import vegoo.stockdata.db.GdhsPersistentService;
import vegoo.stockdata.db.HQPersistentService;
import vegoo.stockdata.db.base.PersistentServiceImpl;

@Component (immediate = true)
public class GdhsPersistentServiceImpl extends PersistentServiceImpl implements GdhsPersistentService, ManagedService{
	private static final Logger logger = LoggerFactory.getLogger(GdhsPersistentServiceImpl.class);

    @Reference private RedisService redis;
    @Reference private JdbcService db;
    @Reference private HQPersistentService dbKDayData; // (policy=ReferencePolicy.DYNAMIC)

    @Activate
    private void activate() {
		logger.info("OSGI BUNDLE:{} activated.", this.getClass().getName());
   	
    }
    
    @Override
	public void updated(Dictionary<String, ?> properties) throws ConfigurationException {
		
		
	}
	public boolean existGDHS(String stockCode, Date endDate) {
		Integer dataTag = queryDataTag(stockCode, endDate);
		return dataTag != null;
	}
	
	private static final String SQL_EXIST_GDHS = "select dataTag from gdhs where stockcode=? and enddate=?";
	private Integer queryDataTag(String stockCode, Date endDate) {
		try {
			return db.queryForObject(SQL_EXIST_GDHS, new Object[] {stockCode, endDate}, 
					new int[] {Types.VARCHAR, Types.DATE}, Integer.class);
		}catch(EmptyResultDataAccessException e) {
			return null;
		}catch(Exception e) {
    		logger.error(String.format("stockCode:%s, endDate:%tF%n", stockCode, endDate), e);
			return null;
		}
	}
	@Override
	public boolean isNewGDHS(String stkCode, Date endDate, int dataTag, boolean deleteOld) {
    	Integer oldDataTag = queryDataTag(stkCode, endDate);
		
		if(deleteOld && (oldDataTag != null) && (oldDataTag != dataTag)) {
			deleteGDHS(stkCode, endDate);
		}
		
		return oldDataTag==null || dataTag != oldDataTag;
	}
	
	private static String SQL_DEL_GDHS = "delete from gdhs where stockcode=? and enddate=?";
    private void deleteGDHS(String stockCode, Date endDate) {
		try {
			 db.update(SQL_DEL_GDHS, new Object[] {stockCode, endDate}, 
					new int[] {Types.VARCHAR, Types.DATE});
		}catch(Exception e) {
			logger.error("",e);
		}
	}

	private static final String SQL_ADDGDHS = "insert into gdhs(stockCode, EndDate, "
			+"HolderNum,PreviousHolderNum,HolderNumChange,HolderNumChangeRate,RangeChangeRate, "
			+"PreviousEndDate,HolderAvgCapitalisation,HolderAvgStockQuantity,TotalCapitalisation, "
			+"CapitalStock,NoticeDate,ClosePrice,dataTag)"         // ,CapitalStockChange,CapitalStockChangeEvent,ClosePrice
			+"values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"; //,?,?,?
	public void insertGdhs(String stockCode, Date endDate, double holderNum, double previousHolderNum,
			double holderNumChange, double holderNumChangeRate, double rangeChangeRate, Date previousEndDate,
			double holderAvgCapitalisation, double holderAvgStockQuantity, double totalCapitalisation,
			double capitalStock, Date noticeDate, int dataTag) {
		
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
		
		double ClosePrice = 0;
		if(holderAvgStockQuantity>0) {
			ClosePrice = holderAvgCapitalisation/holderAvgStockQuantity;
		}
		
		try {
		   db.update( SQL_ADDGDHS, new Object[] {stockCode, endDate, 
				   holderNum,  previousHolderNum,
					 holderNumChange,  holderNumChangeRate,  rangeChangeRate,  previousEndDate,
					 holderAvgCapitalisation,  holderAvgStockQuantity,  totalCapitalisation,
					 capitalStock,  noticeDate,ClosePrice,dataTag}, ///*, item.getCapitalStockChange(), item.getCapitalStockChangeEvent(), item.getClosePrice()*/
				new int[] {Types.VARCHAR, Types.DATE,  Types.DOUBLE,Types.DOUBLE,Types.DOUBLE,
						   Types.DOUBLE, Types.DOUBLE, Types.DATE,  Types.DOUBLE, Types.DOUBLE, Types.DOUBLE,
						   Types.DOUBLE,Types.DATE, Types.DOUBLE, Types.INTEGER});  ///*,Types.DOUBLE,Types.VARCHAR,Types.DOUBLE*/
		}catch(Exception e) {
			logger.error("", e);
		}
	}

	@Override
	public void settleGdhs(boolean reset) {
		if(reset) {
			resetGdhs();
		}
		// 调整报告期对应的交易日；
		settleEndTradeDate();
		// 调整报告期对应的上期数据
		processPrevRData();
	}
	
	private void settleEndTradeDate() {
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

	private void processPrevRData() {
		int years = 8; 
		Date now = new Date();
		for(int i=0; i<4*years; ++i) {
			processPrevRData(StockUtil.getReportDate(now, -i));
		}
	}

	private void processPrevRData(Date reportDate) {
		Date prevRDate = StockUtil.getReportDate(reportDate, -1);
		
		// 查询reportDate季报数据中，上季数据不是prevRDate记录
		List<String> stockCodes = queryNPrevReportGDHS(reportDate, prevRDate);
		
		for(String stkCode:stockCodes) {
			processPrevRData(reportDate, prevRDate, stkCode);
		}
		
	}

	private static final String QRY_NPREV = "select stockcode, HolderNum from gdhs where EndDate = ? and PreviousEndDate != ?";
	private List<String> queryNPrevReportGDHS(Date reportDate, Date prevRDate) {
		try {
			return db.queryForList(QRY_NPREV, 
					new Object[] {reportDate, prevRDate},
					new int[] {Types.DATE, Types.DATE},
					String.class);
		}catch(Exception e) {
		  return new ArrayList<>();
		}
	}
	private static final String UPD_PREV = "update gdhs set PreviousEndDate=?, PreviousHolderNum=?, "
			+ "HolderNumChange=HolderNum-PreviousHolderNum, HolderNumChangeRate=HolderNumChange*100/PreviousHolderNum"
			+ " where stockcode=? and EndDate=?";
	private void processPrevRData(Date reportDate, Date prevRDate, String stkCode) {
		double prevHolderNum = 0;
		Map<String,Object> prevItem = queryGDHS(stkCode, prevRDate);
		if(prevItem != null) {
			prevHolderNum = ((BigDecimal)prevItem.get("HolderNum")).doubleValue();
		}
		
		try {
			db.update(UPD_PREV,
					new Object[] {prevRDate, prevHolderNum, stkCode, reportDate},
					new int[] {Types.DATE, Types.DOUBLE, Types.VARCHAR,Types.DATE});
		}catch(Exception e) {
			logger.error("",e);
		}
	}

	private static final String QRY_GDHS = "select * from gdhs where stockcode = ? and  EndDate= ?";
	@Override
	public Map<String, Object> queryGDHS(String stkCode, Date rDate) {
		try {
			return db.queryForMap(QRY_GDHS, 
					new Object[] {stkCode, rDate},
					new int[] {Types.VARCHAR, Types.DATE});
		}catch(EmptyResultDataAccessException e) {
			return null;
		}catch(Exception e) {
			logger.error("",e);
			return null;
		}
		
	}
	
	private void resetGdhs() {
		// 避免锁表，逐条更新
		while(resetData(1000)==1000);
	}

	private int resetData(int pageSize) {
		List<Integer> rowids = queryNnullData(pageSize);
		for(Integer rowid:rowids) {
			setNullData(rowid);
		}
		return rowids.size();
	}

	private static final String QRY_NNUL_PREV = "select id from gdhs where (PrevRDate is not null) or (EndTradeDate is not null) limit ?";
	private List<Integer> queryNnullData(int pageSize) {
		try {
			return db.queryForList(QRY_NNUL_PREV,
					new Object[] {pageSize},
					new int[] {Types.INTEGER},
					Integer.class);
		}catch(EmptyResultDataAccessException e) {
		   return new ArrayList<>();
		}catch(Exception e) {
		   logger.error("",e);
		   return new ArrayList<>();
		}
	}
	
	private static final String UPD_NNUL_DATA = "update gdhs set PrevRDate=null, EndTradeDate=null where id= ?";
	private void setNullData(Integer rowid) {
		try {
			 db.update(UPD_NNUL_DATA,
					new Object[] {rowid},
					new int[] {Types.INTEGER});
		}catch(Exception e) {
		   logger.error("",e);
		}
	}
	
}
