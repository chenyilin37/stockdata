package vegoo.stockdata.db.jgcc;

import java.math.BigDecimal;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.Dictionary;
import java.util.List;
import java.util.Map;

import org.osgi.service.cm.ConfigurationException;
import org.osgi.service.cm.ManagedService;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.EmptyResultDataAccessException;

import vegoo.commons.jdbc.JdbcService;
import vegoo.commons.redis.RedisService;
import vegoo.stockcommon.utils.StockUtil;
import vegoo.stockdata.db.FhsgPersistentService;
import vegoo.stockdata.db.HQPersistentService;
import vegoo.stockdata.db.JgccPersistentService;
import vegoo.stockdata.db.base.PersistentServiceImpl;

@Component (immediate = true)
public class JgccPersistentServiceImpl extends PersistentServiceImpl implements JgccPersistentService, ManagedService {
	private static final Logger logger = LoggerFactory.getLogger(JgccPersistentServiceImpl.class);
	
    @Reference private RedisService redis;

    @Reference private JdbcService db;

    @Reference private HQPersistentService dbKDayData; // (policy=ReferencePolicy.DYNAMIC)
    @Reference private FhsgPersistentService dbFhsg;   // (policy=ReferencePolicy.DYNAMIC)

	@Override
	public void updated(Dictionary<String, ?> properties) throws ConfigurationException {
		
	}
	
    public boolean existJgcc(String stkcode, Date rdate, int jglx) {
    	Integer dataTag = queryDataTag(stkcode, rdate, jglx);
    	return dataTag != null;
    }
    

	@Override
	public boolean isNewJgcc(String scode, Date reportDate, int lx, int dataTag, boolean deleteOld) {
    	Integer oldDataTag = queryDataTag(scode, reportDate, lx);
		
		if(deleteOld && (oldDataTag != null) && (oldDataTag != dataTag)) {
			deleteJgcc(scode, reportDate, lx);
		}
		
		return oldDataTag==null || dataTag != oldDataTag;
	}
	
	private static String SQL_DEL_JGCC = "delete from jgcc where SCode=? and RDate=? and lx=?";
    private void deleteJgcc(String scode, Date reportDate, int lx) {
		try {
			 db.update(SQL_DEL_JGCC, 
					 new Object[] {scode, reportDate, lx},
					 new int[] {Types.VARCHAR,Types.DATE, Types.INTEGER});
		}catch(Exception e) {
			logger.error("",e);
		}
	}

	private static String SQL_QRY_DATATAG = "select dataTag from jgcc where SCode=? and RDate=? and lx=?";
    private Integer queryDataTag(String stkcode, Date rdate, int jglx) {
		try {
			return db.queryForObject(SQL_QRY_DATATAG, new Object[] {stkcode, rdate, jglx},
					new int[] {Types.VARCHAR,Types.DATE, Types.INTEGER}, Integer.class);
		}catch(EmptyResultDataAccessException e) {
			return null;
		}catch(Exception e) {
			logger.error("",e);
			return null;
		}
	}
    
	private static final String SQL_INS_JGCC = "insert into jgcc(SCODE,RDATE,LX,COUNT,CGChange,ShareHDNum,VPosition,TabRate,LTZB,ShareHDNumChange,RateChange,closeprice,dataTag) "
			                                            + "values (?,?,?,?,?,?,?,?,?,?,?,?,?)";
	//private static final String[] JGCC_FLDS_DB = {F_SCODE,F_RDATE,F_JGLX, F_COUNT, F_CGChange, F_ShareHDNum,F_VPosition,F_TabRate,F_LTZB,F_ShareHDNumChange,F_RateChange}; //LTZBChange
	//private static final String[] JGCC_FLDS_JS = {F_SCODE,F_RDATE,F_JGLX, F_COUNT, F_CGChange, F_ShareHDNum,F_VPosition,F_TabRate,F_LTZB,F_ShareHDNumChange,F_RateChange}; //LTZBChange
	private static final int[] JGCC_FLD_Types  = {Types.VARCHAR,Types.DATE, Types.INTEGER,Types.DOUBLE,Types.VARCHAR,Types.DOUBLE,Types.DOUBLE,Types.DOUBLE,Types.DOUBLE,Types.DOUBLE,Types.DOUBLE,Types.DOUBLE, Types.INTEGER};
	
	public void insertJgcc(String sCODE, Date rDATE, int jglx,double cOUNT,
			String cGChange, double shareHDNum,double vPosition,double tabRate,
			double lTZB,double shareHDNumChange,double rateChanges, int dataTag) {

		if(shareHDNum <1) {  // 小于1的记录，丢弃
		   return;	
		}
		
		double closeprice = vPosition/shareHDNum;
		
		if(closeprice >10000) {  // 单价大于10000，丢弃
		   return;
		}
		
		try {
			db.update( SQL_INS_JGCC, 
					new Object[] {sCODE,rDATE,jglx,cOUNT,cGChange,shareHDNum,vPosition,tabRate,lTZB,shareHDNumChange,rateChanges,closeprice,dataTag}, 
					  JGCC_FLD_Types);
		}catch(Exception e) {
			logger.error("",e);
		}
		
		// updateJgccWithNullPrevRDate(rDATE, sCODE, jglx, cGChange, shareHDNum, lTZB, closeprice);

	}

	@Override
	public void settleJgcc(boolean reset) {
		if(reset) {
			restJgcc();
		}
		
		// 调整报告期对应的交易日；
		settleEndTradeDate();
		// 调整上期数据；
		settlePrevData();
	}
	

	private void settleEndTradeDate() {
		List<Date> reportDates = queryReportDateOfJgccNullTransDate();
		
		for(Date rptDate:reportDates) {
			processJgcc(rptDate);
		}
	}
	
	
	private static final String QRY_NULL_TRNSDATE_JGCC = "select distinct RDate from jgcc where EndTradeDate is null order by RDate desc";
	private List<Date> queryReportDateOfJgccNullTransDate() {
		try {
			return db.queryForList(QRY_NULL_TRNSDATE_JGCC, Date.class);
		}catch(EmptyResultDataAccessException e) {
			return new ArrayList<>();
		}catch(Exception e) {
			logger.error("", e);
			return new ArrayList<>();
		}
	}

	private void processJgcc(Date rptDate) {
		List<Map<String, Object>> items = queryNullTransDateOfJgcc(rptDate);
		if(items==null||items.isEmpty()) {
			return ;
		}
		
		for(Map<String, Object> item:items) {
		  updateNullTransDateOfJgcc(rptDate, item);
		}
		
	}

	private static final String QRY_JGCC_NULLTRNS="select id, scode from jgcc where rdate=? and EndTradeDate is null";
    private List<Map<String, Object>> queryNullTransDateOfJgcc(Date rptDate){
    	try {
    		return db.queryForList(QRY_JGCC_NULLTRNS, new Object[] {rptDate}, 
    				new int[] {Types.DATE});
		}catch(EmptyResultDataAccessException e) {
			return null;
    	}catch(Exception e) {
    		logger.error("",e);
    		return null;
    	}
    }

	private void updateNullTransDateOfJgcc(Date rdate, Map<String, Object> item) {
		String scode = (String) item.get("scode");
		Integer id = (Integer) item.get("id");

		Date endTradeDate = dbKDayData.getLastTradeDate(scode,rdate);
		if(endTradeDate==null) {
			return;
		}
		
		updateNullTransDateOfJGCC(id, endTradeDate);
	}
	
	private static final String UPD_JGCC_TRNSDATE="update jgcc set EndTradeDate=? where id=?";
	private void updateNullTransDateOfJGCC(Integer id, Date endTradeDate) {
		try {
			db.update( UPD_JGCC_TRNSDATE, 
					new Object[] {endTradeDate, id},
					new int[] {Types.DATE, Types.INTEGER});
		}catch(EmptyResultDataAccessException e) {
			return;
		}catch(Exception e) {
			logger.error("",e);
			return;
		}
	}

	private void settlePrevData() {
		// 查询还没有处理上期数据的季报日期
		List<Date> reportDates = queryRDatesOfNullPrevRDate();
		for(Date reportDate : reportDates) {
			// 逐季处理
			settlePrevDataByReportDate(reportDate);
		}
	}

	private static final String QRY_NULL_PRVDATE_JGCC = "select distinct RDate from jgcc where PrevRDate is null order by RDate desc";
	private List<Date> queryRDatesOfNullPrevRDate() {
		try {
			return db.queryForList(QRY_NULL_PRVDATE_JGCC, Date.class);
		}catch(EmptyResultDataAccessException e) {
			return new ArrayList<>();
		}catch(Exception e) {
			logger.error("", e);
			return new ArrayList<>();
		}
	}

	private void settlePrevDataByReportDate(Date reportDate) {
		List<Map<String, Object>> items = queryJgccWithNullPrevRDate(reportDate);
		if(items==null||items.isEmpty()) {
			return ;
		}
		
		for(Map<String, Object> item:items) {
			try {
				String scode = (String) item.get("scode");
				int lx = (int) item.get("lx") ;
				String CGChange = (String) item.get("CGChange");
				double ltzb = ((BigDecimal)item.get("ltzb")).doubleValue();
				long shareHDNum =  ((BigDecimal)item.get("ShareHDNum")).longValue();
				// double Count = (double)item.get("Count");
				double closePrice = ((BigDecimal)item.get("ClosePrice")).doubleValue();
				
			    updateJgccWithNullPrevRDate(reportDate, scode, lx, CGChange, shareHDNum, ltzb, closePrice);
			}catch(Exception e) {
				logger.error("",e);
				continue;
			}
		}
	}

	private static final String QRY_JGCC_NULLPREV="select id, scode, lx, CGChange,ltzb,Count,ShareHDNum,ClosePrice from jgcc where rdate=? and PrevRDate is null";
    private List<Map<String, Object>> queryJgccWithNullPrevRDate(Date rptDate){
    	try {
    		return db.queryForList(QRY_JGCC_NULLPREV, new Object[] {rptDate}, 
    				new int[] {Types.DATE});
		}catch(EmptyResultDataAccessException e) {
			return null;
    	}catch(Exception e) {
    		logger.error("",e);
    		return null;
    	}
    }	
    
	private void updateJgccWithNullPrevRDate(Date reportDate, String scode, int lx, String cGChange, double shareHDNum, double ltzb, double closePrice) {
		Date prevRDate = StockUtil.getReportDate(reportDate, -1);
		
		Map<String, Object> prevItem = null;
		
		if(!"新进".equals(cGChange)) {
			prevItem = queryJgccData(prevRDate, scode, lx);
			//if(prevItem==null) {
			//	return;  数据不完整时，用于留后处理，数据完整了，可去掉；
			//}
		}
		
		updateJgccWithNullPrevRDate(reportDate, scode, lx, shareHDNum, ltzb, closePrice, prevRDate, prevItem);
	}

	private static final String QRY_JGCC="select Count,ShareHDNum,LTZB from jgcc where RDate=? and SCode=? and lx=?";
	private Map<String, Object> queryJgccData(Date rptDate, String scode, int lx) {
    	try {
    		return db.queryForMap(QRY_JGCC, 
    				new Object[] {rptDate, scode, lx}, 
    				new int[] {Types.DATE, Types.VARCHAR, Types.INTEGER});
		}catch(EmptyResultDataAccessException e) {
			return null;
    	}catch(Exception e) {
    		logger.error("",e);
    		return null;
    	}
	}

	private static final String UPD_PREV = "update jgcc set PrevRDate=?,PrevShareHDNum=?,PrevLTZB=?,ShareHDNumChange=?,LTZBChange=?,ValueChange=?,CGChange=? where  RDate=? and SCode=? and lx=?";
	private void updateJgccWithNullPrevRDate(Date reportDate, String scode, int lx, double shareHDNum, double ltzb, double closePrice, Date prevRDate, Map<String, Object> prevItem) {
		double prevHDNum = 0;
		double PrevLTZB=0;
		
		String CGChange = "新进";
		
		if(prevItem != null) {
			PrevLTZB = ((BigDecimal)prevItem.get("LTZB")).doubleValue();
			prevHDNum = ((BigDecimal)prevItem.get("ShareHDNum")).doubleValue();
			// 计算分红送股
			if(prevHDNum>0) {
			   prevHDNum = dbFhsg.adjustWithHoldNum(reportDate, scode, prevHDNum);
			}
			CGChange = "持平";
			if(ltzb > PrevLTZB) {
				CGChange = "增持";
			}else if(ltzb < PrevLTZB){
				CGChange = "减持";
			}
		}
		
		double ShareHDNumChange = shareHDNum-prevHDNum;
		double LTZBChange = ltzb - PrevLTZB;
		double ValueChange = ShareHDNumChange * closePrice;

		try {
			db.update(UPD_PREV, 
    				new Object[] {prevRDate, prevHDNum, PrevLTZB, ShareHDNumChange, LTZBChange, ValueChange,CGChange,
    						reportDate, scode, lx}, 
    				new int[] {Types.DATE,Types.INTEGER,Types.DOUBLE,Types.INTEGER,Types.DOUBLE,Types.DOUBLE,Types.VARCHAR,
    						   Types.DATE,Types.VARCHAR,Types.INTEGER});
		}catch(Exception e) {
			logger.error("",e);
		}
	}



	private void restJgcc() {
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

	private static final String QRY_NNUL_PREV = "select id from jgcc where (PrevRDate is not null) or (EndTradeDate is not null) limit ?";
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
	
	private static final String UPD_NNUL_DATA = "update jgcc set PrevRDate=null, EndTradeDate=null where id= ?";
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
