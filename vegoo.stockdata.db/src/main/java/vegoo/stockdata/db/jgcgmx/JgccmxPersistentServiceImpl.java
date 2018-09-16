package vegoo.stockdata.db.jgcgmx;

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

import vegoo.commons.jdbc.JdbcService;
import vegoo.commons.redis.RedisService;
import vegoo.stockcommon.utils.StockUtil;
import vegoo.stockdata.core.BaseJob;
import vegoo.stockdata.db.FhsgPersistentService;
import vegoo.stockdata.db.JgccmxPersistentService;
import vegoo.stockdata.db.base.PersistentServiceImpl;

@Component (immediate = true)
public class JgccmxPersistentServiceImpl extends PersistentServiceImpl implements JgccmxPersistentService, ManagedService{
	private static final Logger logger = LoggerFactory.getLogger(JgccmxPersistentServiceImpl.class);

    @Reference private RedisService redis;
    @Reference private JdbcService db;
    @Reference private FhsgPersistentService dbFhsg; 

	@Override
	public void updated(Dictionary<String, ?> properties) throws ConfigurationException {
		// TODO Auto-generated method stub
		
	}

	public boolean existJgccmx(String scode, Date rdate, String shcode) {
    	Integer dataTag = queryDataTag(scode, rdate, shcode);
    	return dataTag != null;
	}
	
	@Override
	public boolean isNewJgccmx(String scode, Date rdate, String shcode, int dataTag, boolean deleteOld) {
    	Integer oldDataTag = queryDataTag(scode, rdate, shcode);
		
		if(deleteOld && (oldDataTag != null) && (oldDataTag != dataTag)) {
			deleteJgccmx(scode, rdate, shcode);
		}
		
		return oldDataTag==null || dataTag != oldDataTag;
	}
	
	private static String QRY_JGCCMX_TAG = "select dataTag from jgccmx where SCode=? and RDate=? and SHCode=?";
	public Integer queryDataTag(String scode, Date rdate, String shcode) {
		try {
			
			return db.queryForObject(QRY_JGCCMX_TAG, 
					new Object[] {scode, rdate, shcode},
    				new int[] {Types.VARCHAR,Types.DATE, Types.VARCHAR}, 
    				Integer.class);
		}catch(EmptyResultDataAccessException e) {
			return null;
    	}catch(Exception e) {
    		logger.error("", e);
    		return null;
    	}
    }

	
	private static String SQL_DEL_JGCCMX = "delete from jgccmx where SCode=? and RDate=? and SHCode=?";
    public void deleteJgccmx(String scode, Date rdate, String shcode) {
		try {
			 db.update(SQL_DEL_JGCCMX, 
					 new Object[] {scode, rdate, shcode},
					 new int[] {Types.VARCHAR,Types.DATE, Types.VARCHAR});
		}catch(Exception e) {
			logger.error("",e);
		}
	}
	
    //SCode,SName,RDate,SHCode,SHName,IndtCode,InstSName,TypeCode,Type,ShareHDNum,Vposition,TabRate,TabProRate
	private static final String SQL_INS_JGCCMX = "insert into jgccmx(SCode,RDate,SHCode,IndtCode,TypeCode,ShareHDNum,Vposition,TabRate,TabProRate,ClosePrice,dataTag) "
			                                               + "values (?,?,?,?,?,?,?,?,?,?,?)";
	//private static final String[] MX_FLDS_DB = {"SCode","RDate","SHCode","IndtCode","TypeCode","ShareHDNum","Vposition","TabRate","TabProRate"};
	// private static final String[] MX_FLDS_JSON = {F_MX_SCode,F_MX_RDate,F_MX_SHCode,F_MX_IndtCode,F_MX_TypeCode,F_MX_ShareHDNum,F_MX_Vposition,F_MX_TabRate,F_MX_TabProRate}; 
	private static final int[] MX_FLD_Types  = {Types.VARCHAR,Types.DATE, Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,Types.DOUBLE,Types.DOUBLE,Types.DOUBLE,Types.DOUBLE,Types.DOUBLE,Types.INTEGER};
	
	public void insertJgccmx(String SCode, Date RDate, String SHCode,String IndtCode,String TypeCode,double shareHDNum,double vPosition,double TabRate,double TabProRate, int dataTag ) {
		if(shareHDNum <1) {  // 小于1的记录，丢弃
			   return;	
		}
		
		double closeprice = vPosition/shareHDNum;
		
		if(closeprice >10000) {  // 单价大于10000，丢弃
		   return;
		}
		
		try {
			db.update( SQL_INS_JGCCMX, 
					  new Object[] {SCode,RDate,SHCode,IndtCode,TypeCode,shareHDNum,vPosition,TabRate,TabProRate, closeprice, dataTag}, 
					  MX_FLD_Types);
		}catch(Exception e) {
			logger.error("",e);
		}
	}

	@Override
	public void settleJgccmx(boolean reset) {
		if(reset) {
			resetJgccmx();
		}
		
		List<Date> reportDates = queryJgccmxReportDates();
		
		for(int i=0; i<reportDates.size()-1; ++i) { // 最早的季报不用算
			Date rdate = reportDates.get(i);
			processJgccmx(rdate);
		}
	}
	

	// 日期降序排列
	private static final String QRY_JGCCMX_RDATE="SELECT distinct rdate FROM jgccmx order by rdate desc";
	private List<Date> queryJgccmxReportDates() {
		try {
			return db.queryForList(QRY_JGCCMX_RDATE, Date.class);
		}catch(EmptyResultDataAccessException e) {
			return new ArrayList<>();
		}catch(Exception e) {
			logger.error("", e);
			return new ArrayList<>();
		}
	}

	private void processJgccmx(Date rdate) {
		List<Map<String, Object>> items = queryJgccmxData(rdate);
		Date prevRDate = StockUtil.getReportDate(rdate, -1);
		
		for(Map<String, Object> item : items) {
			updateJgccmxData(rdate, item, prevRDate);
		}
	}

	private static final String QRY_NULL_JGCCMX="SELECT id,SCode,SHCode FROM jgccmx where rdate=? and PrevRDate is null";
	private List<Map<String, Object>> queryJgccmxData(Date rdate){
		try {
			return db.queryForList(QRY_NULL_JGCCMX, new Object[] {rdate}, new int[] {Types.DATE});
		}catch(EmptyResultDataAccessException e) {
			return new ArrayList<>();
		}catch(Exception e) {
			logger.error("", e);
			return new ArrayList<>();
		}
	}
	
	private void updateJgccmxData(Date theRDate, Map<String, Object> item, Date prevRDate) {
		Integer id = (Integer) item.get("id");
		String SCode = (String) item.get("SCode");
		String SHCode = (String) item.get("SHCode");
		//long shareHDNum = ((BigDecimal) item.get("SHCode")).longValue();
		
		Map<String, Object> prevItem = queryJgccmx(SCode, prevRDate, SHCode);
		if(prevItem==null || prevItem.isEmpty()) {
			return;
		}
		updateJgccmxPrevData(id, theRDate, SCode, prevRDate, prevItem);
	}

	/***
	 * 将机构持股明细，补上上期持股数据，便于分析
	 * @param rdate
	 */
	private static final String SQL_QRY_JGCCMX_PREV="SELECT * FROM jgccmx where scode=? and rdate=? and shcode=?";
	public Map<String, Object> queryJgccmx( String SCode, Date rdate,String SHCode){
		try {
			return db.queryForMap(SQL_QRY_JGCCMX_PREV, 
					new Object[] {SCode,rdate, SHCode}, 
					new int[] {Types.VARCHAR, Types.DATE, Types.VARCHAR});
		}catch(EmptyResultDataAccessException e) {
			return null;
		}catch(Exception e) {
			logger.error("", e);
			return null;
		}
	}
	
	private static final String UPD_JGCCMX_PREV="UPDATE jgccmx set PrevRDate=?, PrevHDNum=?, PrevLTZB=?, PrevVPosition=?,"
	        +" ChangeHDNum=ShareHDNum-PrevHDNum , ChangeValue=ChangeHDNum*ClosePrice,ChangeLTZB=TabProRate-PrevLTZB "
			+" where id=?";
	private void updateJgccmxPrevData(Integer id, Date theRDate, String scode, Date prevRDate, Map<String, Object> prevItem) {
		try {
			double prevHDNum = ((BigDecimal)prevItem.get("ShareHDNum")).doubleValue();
			
			if(prevHDNum>0) {
			   prevHDNum = dbFhsg.adjustWithHoldNum(theRDate, scode, prevHDNum);
			}
			
			double prevLtzb = ((BigDecimal)prevItem.get("TabProRate")).doubleValue();
			double prevValue = ((BigDecimal)prevItem.get("Vposition")).doubleValue();
			
			db.update( UPD_JGCCMX_PREV, 
					new Object[] {prevRDate, prevHDNum, prevLtzb, prevValue, 
							id}, 
					new int[] {Types.DATE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, 
							   Types.INTEGER});
		}catch(Exception e) {
			logger.error("", e);
		}
	}
	
	private void resetJgccmx() {
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

	private static final String QRY_NNUL_PREV = "select id from jgccmx where PrevRDate is not null limit ?";
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
	
	private static final String UPD_NNUL_PREV = "update jgccmx set PrevRDate=null where id= ?";
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
