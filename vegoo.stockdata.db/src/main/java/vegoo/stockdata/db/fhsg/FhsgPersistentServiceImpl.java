package vegoo.stockdata.db.fhsg;

import java.math.BigDecimal;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.osgi.service.cm.ConfigurationException;
import org.osgi.service.cm.ManagedService;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.EmptyResultDataAccessException;

import vegoo.jdbcservice.JdbcService;
import vegoo.redis.RedisService;
import vegoo.stockdata.core.model.FhsgItemDao;
import vegoo.stockdata.core.utils.StockUtil;
import vegoo.stockdata.db.FhsgPersistentService;
import vegoo.stockdata.db.HQPersistentService;
import vegoo.stockdata.db.base.PersistentServiceImpl;

@Component (immediate = true)
public class FhsgPersistentServiceImpl extends PersistentServiceImpl implements FhsgPersistentService, ManagedService {
	private static final Logger logger = LoggerFactory.getLogger(FhsgPersistentServiceImpl.class);

    @Reference private RedisService redis;
    @Reference private JdbcService db;
    @Reference private HQPersistentService dbKDayData; // (policy=ReferencePolicy.DYNAMIC)

	private static final Map<String,Map<Date,FhsgItemDao>> cqcxCache = new HashMap<>();

	@Override
	public void updated(Dictionary<String, ?> properties) throws ConfigurationException {
		
	}
 	
 	@Activate
 	public void activate() {
 		loadCqcxData();
 	}
 	
	private void loadCqcxData() {
		
		
	}
	
	@Override
	public boolean isNewFhsg(String stockCode, Date rDate, int dataTag, boolean deleteOld) {
    	Integer oldDataTag = queryDataTag(stockCode, rDate);
		
		if(deleteOld && (oldDataTag != null) && (oldDataTag != dataTag)) {
			deleteFhsg(stockCode, rDate);
		}
		
		return oldDataTag==null || dataTag != oldDataTag;
	}
	

	public boolean existFhsg(String stockCode, Date rDate) {
		Integer dataTag = queryDataTag(stockCode, rDate);
		return dataTag != null;
	}

	private static final String SQL_FHSG_TAG = "select dataTag from fhsg where scode=? and rdate=?";
	private Integer queryDataTag(String stockCode, Date rDate) {
		try {
			return db.queryForObject(SQL_FHSG_TAG, 
					new Object[] {stockCode, rDate}, 
					new int[] {Types.VARCHAR, Types.DATE}, 
					Integer.class);
		}catch(EmptyResultDataAccessException e) {
			return null;
		}catch(Exception e) {
    		logger.error(String.format("stockCode:%s, endDate:%tF%n", stockCode, rDate), e);
			return null;
		}
	}
	
	private static final String SQL_DEL_FHSG = "delete from fhsg where scode=? and rdate=?";
	public void deleteFhsg(String stockCode, Date rDate) {
		try {
			 db.update(SQL_DEL_FHSG, 
					new Object[] {stockCode, rDate}, 
					new int[] {Types.VARCHAR, Types.DATE});
		}catch(Exception e) {
    		logger.error(String.format("stockCode:%s, endDate:%tF%n", stockCode, rDate), e);
		}
		
	}
	
	
	private static final String SQL_ADD_FHSG = "insert into fhsg(SCode,Rdate,SZZBL,SGBL,ZGBL,XJFH,GXL,YAGGR,GQDJR,CQCXR,YAGGRHSRZF,GQDJRQSRZF,CQCXRHSSRZF,TotalEquity,"
			+ "EarningsPerShare,NetAssetsPerShare,MGGJJ,MGWFPLY,JLYTBZZ,ResultsbyDate,ProjectProgress,AllocationPlan,YCQTS, dataTag) "
			+ "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
	
	@Override
	public void insertFhsg(FhsgItemDao dao) {
		try {
		   db.update( SQL_ADD_FHSG, new Object[] {dao.getSCode(), dao.getRDate(),
				dao.getSZZBL(), dao.getSGBL(), dao.getZGBL(), dao.getXJFH(), dao.getGXL(),
				dao.getYAGGR(), dao.getGQDJR(), dao.getCQCXR(),
				dao.getYAGGRHSRZF(), dao.getGQDJRQSRZF(), dao.getCQCXRHSSRZF(),
				dao.getTotalEquity(), dao.getEarningsPerShare(), dao.getNetAssetsPerShare(),
				dao.getMGGJJ(), dao.getMGWFPLY(), dao.getJLYTBZZ(), dao.getResultsbyDate(),
				dao.getProjectProgress(), dao.getAllocationPlan(), dao.getYCQTS(), dao.getDataTag()}, 
				new int[] {Types.VARCHAR, Types.DATE,  
						   Types.DOUBLE,Types.DOUBLE,Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, 
						   Types.DATE,  Types.DATE, Types.DATE,
						   Types.DOUBLE, Types.DOUBLE, Types.DOUBLE,Types.DOUBLE,Types.DOUBLE,
						   Types.DOUBLE, Types.DOUBLE, Types.DOUBLE,Types.DOUBLE,
						   Types.DATE,Types.VARCHAR, Types.VARCHAR,Types.DOUBLE, Types.INTEGER});  
		}catch(Exception e) {
			logger.error("", e);
		}
   }

	private static final String QRY_CQCX = "SELECT szzbl FROM fhsg where SCode=? and (CQCXR>? and CQCXR<=?) and szzbl != 0";
	@Override
	public double adjustWithHoldNum(Date reportDate, String scode, double hdNum) {
		Date prevRDate = StockUtil.getReportDate(reportDate, -1);
		
		try {
			List<Map<String,Object>> cqcxItems = db.queryForList(QRY_CQCX,
					new Object[] {scode, prevRDate, reportDate},
					new int[] {Types.VARCHAR, Types.DATE, Types.DATE});
			if(!cqcxItems.isEmpty()) {
				return adjustWithHoldNum(hdNum, cqcxItems);
			}
		}catch(Exception e) {
			logger.error("",e);
		}
		
		return hdNum;
	}
	
	private double adjustWithHoldNum(double hdNum, List<Map<String, Object>> cqcxItems) {
		for(Map<String, Object> item: cqcxItems) {
			BigDecimal szzbl = (BigDecimal) item.get("szzbl");
			hdNum *= (1.0 + szzbl.doubleValue()/10);
		}
		return Math.round(hdNum);
	}

	private static FhsgItemDao getFhsgItemDao(String stockcode, Date tdate) {
		Map<Date, FhsgItemDao>  items = cqcxCache.get(stockcode);
		if(items==null) {
			return null;
		}
		return items.get(tdate);
	}
	
	@Override
	public double calcLClose(String stockcode, Date transdate, double lClose) {
		FhsgItemDao cqcxItem = getFhsgItemDao(stockcode,transdate);
		if(cqcxItem==null) {
			return lClose;
		}
		return lClose + cqcxItem.getAdjustPrice();
	}
	
	@Override
	public void settleFhsg(boolean reset) {
		if(reset) {
			resetData();
		}
		
		List<Map<String,Object>> items = queryNeedSettleItems();
		if(items!=null && !items.isEmpty()) {
			processAdjustPrice(items);
		}
	}



	private static final String QRY_NEED_ITEMS = "select id,SCode,CQCXR,GQDJR,SZZBL,SGBL,ZGBL,XJFH,GXL from fhsg where CQCXR is not null and GQDJR is not null and (adjustPrice=0 or adjustPrice is null)";
	private List<Map<String, Object>> queryNeedSettleItems() {
		try {
			return db.queryForList(QRY_NEED_ITEMS);
		}catch(Exception e) {
			return null;
		}
	}

	private void processAdjustPrice(List<Map<String,Object>> items) {
		for(Map<String,Object> item:items) {
			processAdjustPrice(item);
		}
		
	}

	private void processAdjustPrice(Map<String, Object> item) {
		Integer id = (Integer) item.get("id");
		String SCode = (String) item.get("SCode");
		Date CQCXR = (Date) item.get("CQCXR");
		Date GQDJR = (Date) item.get("GQDJR");
		double SZZBL = ((BigDecimal) item.get("SZZBL")).doubleValue();
		double XJFH = ((BigDecimal) item.get("XJFH")).doubleValue();
		
		
		double lClosePrice = dbKDayData.getClosePrice(SCode,GQDJR);  // 股权登记日收盘价
		if(lClosePrice == 0) {
			return;
		}
		
		double adjustPrice = calcAdjustPrice(SCode, SZZBL, XJFH,lClosePrice);
		
		updateCQdata(id, lClosePrice,adjustPrice);
	}

	private double calcAdjustPrice(String sCode, double sZZBL, double xJFH, double lClosePrice) {
		// 统一按沪市计算方式 //TODO 
		return (lClosePrice-xJFH/10)/(1+sZZBL/10);
	}

	private static final String UPD_CQCX_DATA = "update fhsg set LClosePrice=?,adjustPrice=? where id=?";
	private void updateCQdata(Integer id, double lClosePrice, double adjustPrice) {
		try {
			db.update(UPD_CQCX_DATA,
					new Object[] {lClosePrice,adjustPrice,id},
					new int[] {Types.DOUBLE,Types.DOUBLE, Types.INTEGER});
		}catch(Exception e) {
			logger.error("",e);
		}
		
	}
	
	private void resetData() {
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

	private static final String QRY_NNUL_PREV = "select id from fhsg where adjustPrice!=0 or adjustPrice is not null limit ?";
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
	
	private static final String UPD_NNUL_PREV = "update fhsg set adjustPrice=0 where id= ?";
	private void setNullData(Integer rowid) {
		try {
			 db.update(UPD_NNUL_PREV,
					new Object[] {rowid},
					new int[] {Types.INTEGER});
		}catch(Exception e) {
		   logger.error("",e);
		}
	}
	
}
