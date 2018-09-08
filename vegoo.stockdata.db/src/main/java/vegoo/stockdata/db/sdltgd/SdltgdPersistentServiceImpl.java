package vegoo.stockdata.db.sdltgd;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.EmptyResultDataAccessException;

import vegoo.jdbcservice.JdbcService;
import vegoo.redis.RedisService;
import vegoo.stockdata.core.utils.StockUtil;
import vegoo.stockdata.db.FhsgPersistentService;
import vegoo.stockdata.db.HQPersistentService;
import vegoo.stockdata.db.SdltgdPersistentService;
import vegoo.stockdata.db.base.PersistentServiceImpl;

@Component (immediate = true)
public class SdltgdPersistentServiceImpl  extends PersistentServiceImpl implements SdltgdPersistentService, ManagedService{
	private static final Logger logger = LoggerFactory.getLogger(SdltgdPersistentServiceImpl.class);

    @Reference private RedisService redis;
    @Reference private JdbcService db;
    @Reference private FhsgPersistentService dbFhsg; 
    @Reference private HQPersistentService dbKDayData; // (policy=ReferencePolicy.DYNAMIC)

	@Override
	public void updated(Dictionary<String, ?> properties) throws ConfigurationException {
		
		
	}
	
	@Override
	public boolean isNewSdltgd(String scode, Date rdate, String shcode, int dataTag, boolean deleteOld) {
    	Integer oldDataTag = queryDataTag(scode, rdate, shcode);
		
		if(deleteOld && (oldDataTag != null) && (oldDataTag != dataTag)) {
			deleteSdltgd(scode, rdate, shcode);
		}
		
		return oldDataTag==null || dataTag != oldDataTag;
	}

	public boolean existSdltgd(String scode, Date rdate, String shcode) {
		Integer dataTag = queryDataTag(scode, rdate, shcode);
		return dataTag!=null;
	}
	
	private static String QRY_SDLTGD_TAG = "select dataTag from sdltgd where SCode=? and RDate=? and SHAREHDCODE=?";
	private Integer queryDataTag(String scode, Date rdate, String shcode) {
    	try {
    		return db.queryForObject(QRY_SDLTGD_TAG, 
    				new Object[] {scode, rdate, shcode},
    				new int[] {Types.VARCHAR, Types.DATE, Types.VARCHAR}, 
    				Integer.class);
		}catch(EmptyResultDataAccessException e) {
			return null;
    	}catch(Exception e) {
    		logger.error("", e);
    		return null;
    	}
	}
	
	private static String SQL_DEL_SDLTGD = "delete from sdltgd where SCode=? and RDate=? and SHAREHDCODE=?";
	public void deleteSdltgd(String scode, Date rdate, String shcode) {
    	try {
    		 db.update(SQL_DEL_SDLTGD, 
    				new Object[] {scode, rdate, shcode},
    				new int[] {Types.VARCHAR, Types.DATE, Types.VARCHAR});
    	}catch(Exception e) {
    		logger.error("", e);
    	}
	}
	
	// SSNAME,SNAME
	private static String SQL_ADD_SDLTGD = "insert into sdltgd(COMPANYCODE,SHAREHDNAME,SHAREHDTYPE,SHARESTYPE,"
	         +"RANK,SCODE,RDATE,SHAREHDNUM,LTAG,ZB,NDATE,BZ,BDBL,SHAREHDCODE,SHAREHDRATIO,BDSUM,closeprice,dataTag)"
			 + " values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
	public void insertSdltgd(String companycode, String sharehdname, String sharehdtype, String sharestype,
			double rank, String scode, Date rdate, double sharehdnum, double ltag, double zb, Date ndate, String bz,
			double bdbl, String sharehdcode, double sharehdratio, double bdsum, int dataTag) {
		    
		    if(sharehdnum <1) {  // 小于1的记录，丢弃
			   return;	
			}
			
			double closeprice = ltag/sharehdnum;
			
			if(closeprice >10000) {  // 单价大于10000，丢弃
			   return;
			}

			try {
			    db.update( SQL_ADD_SDLTGD, new Object[] {companycode, sharehdname, sharehdtype, sharestype,
					 rank, scode,  rdate,  sharehdnum,  ltag,  zb,  ndate,  bz,
					 bdbl, sharehdcode,  sharehdratio,  bdsum, closeprice, dataTag},
					new int[] {Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,Types.DOUBLE,
							Types.VARCHAR,Types.DATE,Types.DOUBLE,Types.DOUBLE,Types.DOUBLE,Types.DATE,
							Types.VARCHAR,Types.DOUBLE,Types.VARCHAR,Types.DOUBLE,Types.DOUBLE,Types.DOUBLE,Types.INTEGER});
	
			}catch(Exception e) {
				logger.error("",e);
			}
	}
	
	@Override
	public void settleSdltgd(boolean reset) {
		if(reset) {
			resetData();

		}
		
		// 填写上期数据
		processReportData();
	}
	
	private void processReportData() {
		List<Date> reportDates = queryReportDates();
		
		for(int i=0;i<reportDates.size()-1;++i) { // 最早的日期不用算
			Date rdate = reportDates.get(i);
			processReportData(rdate);
		}
		
	}


	private static final String SQL_QRY_TOP10_RDATE="SELECT distinct rdate FROM sdltgd order by rdate desc";
	private List<Date> queryReportDates() {
		try {
			return db.queryForList(SQL_QRY_TOP10_RDATE, Date.class);
		}catch(EmptyResultDataAccessException e) {
			return new ArrayList<>();
		}catch(Exception e) {
			logger.error("", e);
			return new ArrayList<>();
		}
	}
	
	private void processReportData(Date rdate) {
		processPrevData(rdate);
		processTradeDate(rdate);
		
	}
	
	private void processTradeDate(Date rdate) {
		List<Map<String, Object>> items = queryNullTransDateOfSdltgd(rdate);
		if(items==null||items.isEmpty()) {
			return ;
		}
		
		for(Map<String, Object> item:items) {
		  updateNullTransDateOfSdltgd(rdate, item);
		}
	}

	private static final String QRY_JGCC_NULLTRNS="select id, scode from sdltgd where rdate=? and EndTradeDate is null";
    private List<Map<String, Object>> queryNullTransDateOfSdltgd(Date rptDate){
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

	private void updateNullTransDateOfSdltgd(Date rdate, Map<String, Object> item) {
		String scode = (String) item.get("scode");
		Integer id = (Integer) item.get("id");

		Date endTradeDate = dbKDayData.getLastTradeDate(scode,rdate);
		if(endTradeDate==null) {
			return;
		}
		
		updateNullTransDateOfSdltgd(id, endTradeDate);
		
	}

	private static final String UPD_T10_TRNSDATE="update sdltgd set EndTradeDate=? where id=?";
	private void updateNullTransDateOfSdltgd(Integer id, Date endTradeDate) {
		try {
			db.update( UPD_T10_TRNSDATE, 
					new Object[] {endTradeDate, id},
					new int[] {Types.DATE, Types.INTEGER});
		}catch(EmptyResultDataAccessException e) {
			return;
		}catch(Exception e) {
			logger.error("",e);
			return;
		}
	}

	private void processPrevData(Date rdate) {	
		List<Map<String, Object>> items = queryDataWithNullPrev(rdate);
		
		Date prevRDate = StockUtil.getReportDate(rdate, -1);
		
		for(Map<String, Object> item : items) {
			updateReportData(rdate, item, prevRDate);
		}
	}

	private static final String QRY_NULL_PREV="SELECT id,SCode,SHAREHDCODE,BZ FROM sdltgd where rdate=? and PrevRDate is null";
	private List<Map<String, Object>> queryDataWithNullPrev(Date rdate){
		try {
			return db.queryForList(QRY_NULL_PREV, new Object[] {rdate}, new int[] {Types.DATE});
		}catch(EmptyResultDataAccessException e) {
			return new ArrayList<>();
		}catch(Exception e) {
			logger.error("", e);
			return new ArrayList<>();
		}
	}



	private void updateReportData(Date theRDate, Map<String, Object> item, Date prevRDate) {
		Integer id = (Integer) item.get("id");
		String SCode = (String) item.get("SCode");
		String SHCode = (String) item.get("SHAREHDCODE");
		String bz = (String)item.get("BZ");
		
		Map<String, Object> prevItem = null;
		if(!"新进".equalsIgnoreCase(bz.trim())) {
			prevItem = queryPrevData(prevRDate, SHCode, SCode);
/*			if(prevItem==null || prevItem.isEmpty()) {
				return;  数据不完整时，用于留后处理，数据完整了，可去掉；
			}
*/		}
		updateWithPrevData(id, theRDate, SCode, prevRDate, prevItem);
	}

	// 股东有可能在某个季度跌出前十，但还持股,rdate采用<=prevDate，
	private static final String QRY_PREVDATA="SELECT rdate,ShareHDNum,LTAG,ZB FROM sdltgd where rdate<=? and SHAREHDCODE=? and scode=? limit 1";
	private Map<String, Object> queryPrevData(Date rdate, String sHCode, String sCode) {
		try {
			 
			return db.queryForMap(QRY_PREVDATA, new Object[] {rdate, sHCode, sCode}, 
					new int[] {Types.DATE, Types.VARCHAR, Types.VARCHAR});
		}catch(EmptyResultDataAccessException e) {
			return null;
		}catch(Exception e) {
			logger.error("", e);
			return null;
		}
	}
	
	private static final String UPD_WITH_PREVDATA="UPDATE sdltgd set PrevRDate=?, PrevHDNum=?,"
	        +" PrevLTZB=?, PrevVPosition=?, ChangeHDNum=ShareHDNum-PrevHDNum , ChangeValue=ChangeHDNum*ClosePrice,ChangeLTZB=ZB-PrevLTZB "
			+" where id=?";
	private void updateWithPrevData(Integer id, Date theRDate, String sCode, Date prevRDate, Map<String, Object> prevItem) {
		try {
			double prevHDNum = 0;
			double prevLtzb = 0;
			double prevValue = 0;
			
			if(prevItem != null) {
				prevHDNum= ((BigDecimal)prevItem.get("ShareHDNum")).doubleValue();
				if(prevHDNum>0) {
					prevHDNum = dbFhsg.adjustWithHoldNum(theRDate, sCode, prevHDNum);
				}
				
				prevLtzb = ((BigDecimal)prevItem.get("ZB")).doubleValue();
				prevValue = ((BigDecimal)prevItem.get("LTAG")).doubleValue();
				prevRDate = (Date)prevItem.get("rdate");  
			}
			
			db.update( UPD_WITH_PREVDATA, 
					new Object[] {prevRDate, prevHDNum, prevLtzb, prevValue, id}, 
					new int[] {Types.DATE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.INTEGER});
		}catch(Exception e) {
			logger.error("", e);
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

	private static final String QRY_NNUL_PREV = "select id from sdltgd where (PrevRDate is not null) or (EndTradeDate is not null) limit ?";
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
	
	private static final String UPD_NNUL_PREV = "update sdltgd set PrevRDate=null,EndTradeDate=null where id= ?";
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
