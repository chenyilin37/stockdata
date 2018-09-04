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
import vegoo.stockdata.db.SdltgdPersistentService;
import vegoo.stockdata.db.base.PersistentServiceImpl;

@Component (immediate = true)
public class SdltgdPersistentServiceImpl  extends PersistentServiceImpl implements SdltgdPersistentService, ManagedService{
	private static final Logger logger = LoggerFactory.getLogger(SdltgdPersistentServiceImpl.class);

    @Reference private RedisService redis;
    @Reference private JdbcService db;
    @Reference private FhsgPersistentService dbFhsg; 

	@Override
	public void updated(Dictionary<String, ?> properties) throws ConfigurationException {
		
		
	}
	
	
	
	private static String SQL_EXIST_SDLTGD = "select SCode from sdltgd where SCode=? and RDate=? and SHAREHDCODE=?";
	public boolean existSdltgd(String scode, Date rdate, String shcode) {
		
    	try {
    		String val = db.queryForObject(SQL_EXIST_SDLTGD, new Object[] {scode, rdate, shcode},
    				new int[] {Types.VARCHAR, Types.DATE, Types.VARCHAR}, String.class);
		    return  val != null;
		}catch(EmptyResultDataAccessException e) {
			return false;
    	}catch(Exception e) {
    		logger.error("", e);
    		return false;
    	}
	}
	
	// SSNAME,SNAME
	private static String SQL_ADD_SDLTGD = "insert into sdltgd(COMPANYCODE,SHAREHDNAME,SHAREHDTYPE,SHARESTYPE,"
	         +"RANK,SCODE,RDATE,SHAREHDNUM,LTAG,ZB,NDATE,BZ,BDBL,SHAREHDCODE,SHAREHDRATIO,BDSUM,closeprice)"
			 + " values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
	public void insertSdltgd(String companycode, String sharehdname, String sharehdtype, String sharestype,
			double rank, String scode, Date rdate, double sharehdnum, double ltag, double zb, Date ndate, String bz,
			double bdbl, String sharehdcode, double sharehdratio, double bdsum) {
		    
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
					 bdbl, sharehdcode,  sharehdratio,  bdsum, closeprice},
					new int[] {Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,Types.DOUBLE,
							Types.VARCHAR,Types.DATE,Types.DOUBLE,Types.DOUBLE,Types.DOUBLE,Types.DATE,
							Types.VARCHAR,Types.DOUBLE,Types.VARCHAR,Types.DOUBLE,Types.DOUBLE,Types.DOUBLE});
	
			}catch(Exception e) {
				logger.error("",e);
			}
	}
	
	
	
	@Override
	public void setdbSdltgd() {
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
		
		Map<String, Object> prevItem = null;
		if(!"新进".equals(item.get("BZ"))) {
			prevItem = queryPrevData(prevRDate, SHCode, SCode);
			if(prevItem==null || prevItem.isEmpty()) {
				return;
			}
		}
		updateWithPrevData(id, theRDate, SCode, prevRDate, prevItem);
	}




	private static final String QRY_PREVDATA="SELECT ShareHDNum,LTAG,ZB FROM sdltgd where rdate=? and SHAREHDCODE=? and scode=?";
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
			}
			
			db.update( UPD_WITH_PREVDATA, 
					new Object[] {prevRDate, prevHDNum, prevLtzb, prevValue, id}, 
					new int[] {Types.DATE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.INTEGER});
		}catch(Exception e) {
			logger.error("", e);
		}		
	}


	

		
}
