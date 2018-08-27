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

import vegoo.jdbcservice.JdbcService;
import vegoo.redis.RedisService;
import vegoo.stockdata.core.BaseJob;
import vegoo.stockdata.core.utils.StockUtil;
import vegoo.stockdata.db.JgccmxPersistentService;
import vegoo.stockdata.db.base.PersistentServiceImpl;

@Component (immediate = true)
public class JgccmxPersistentServiceImpl extends PersistentServiceImpl implements JgccmxPersistentService, ManagedService{
	private static final Logger logger = LoggerFactory.getLogger(JgccmxPersistentServiceImpl.class);

    @Reference private RedisService redis;

    @Reference private JdbcService db;


	@Override
	public void updated(Dictionary<String, ?> properties) throws ConfigurationException {
		// TODO Auto-generated method stub
		
	}


	private static String SQL_EXIST_JGCCMX = "select SCode from jgccmx where SCode=? and RDate=? and SHCode=?";
	public boolean existJgccmx(String scode, Date rdate, String shcode) {
    	try {
    		String val = db.queryForObject(SQL_EXIST_JGCCMX, new Object[] {scode, rdate, shcode},
    				new int[] {Types.VARCHAR,Types.DATE, Types.VARCHAR}, String.class);
		    return  val != null;
		}catch(EmptyResultDataAccessException e) {
			return false;
    	}catch(Exception e) {
    		logger.error("", e);
    		return false;
    	}
    }


    //SCode,SName,RDate,SHCode,SHName,IndtCode,InstSName,TypeCode,Type,ShareHDNum,Vposition,TabRate,TabProRate
	private static final String SQL_INS_JGCCMX = "insert into jgccmx(SCode,RDate,SHCode,IndtCode,TypeCode,ShareHDNum,Vposition,TabRate,TabProRate) "
			                                               + "values (?,?,?,?,?,?,?,?,?)";
	//private static final String[] MX_FLDS_DB = {"SCode","RDate","SHCode","IndtCode","TypeCode","ShareHDNum","Vposition","TabRate","TabProRate"};
	// private static final String[] MX_FLDS_JSON = {F_MX_SCode,F_MX_RDate,F_MX_SHCode,F_MX_IndtCode,F_MX_TypeCode,F_MX_ShareHDNum,F_MX_Vposition,F_MX_TabRate,F_MX_TabProRate}; 
	private static final int[] MX_FLD_Types  = {Types.VARCHAR,Types.DATE, Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,Types.DOUBLE,Types.DOUBLE,Types.DOUBLE,Types.DOUBLE};
	
	public void insertJgccmx(String SCode, Date RDate, String SHCode,String IndtCode,String TypeCode,double ShareHDNum,double Vposition,double TabRate,double TabProRate ) {
		
		try {
			db.update( SQL_INS_JGCCMX, 
					  new Object[] {SCode,RDate,SHCode,IndtCode,TypeCode,ShareHDNum,Vposition,TabRate,TabProRate}, 
					  MX_FLD_Types);
		}catch(Exception e) {
			logger.error("",e);
		}
	}

	@Override
	public void settleJgccmx() {
		List<Date> reportDates = queryJgccmxReportDates();
		
		for(int i=0;i<reportDates.size()-1;++i) { // 最早的日期不用算
			Date rdate = reportDates.get(i);
			processJgccmx(rdate);
		}
	}
	
	private static final String SQL_QRY_JGCCMX_RDATE="SELECT distinct rdate FROM jgccmx order by rdate desc";
	private List<Date> queryJgccmxReportDates() {
		try {
			return db.queryForList(SQL_QRY_JGCCMX_RDATE, Date.class);
		}catch(Exception e) {
			logger.error("", e);
			return new ArrayList<>();
		}
	}

	private void processJgccmx(Date rdate) {
		List<Map<String, Object>> items = queryJgccmx(rdate);
		Date prevRDate = StockUtil.getPreviousReportDate(rdate);
		
		for(Map<String, Object> item : items) {
			updateJgccmxData(item, prevRDate);
		}
	}

	private static final String SQL_QRY_JGCCMX="SELECT id,SCode,SHCode FROM jgccmx where rdate=? and PrevRDate is null";
	private List<Map<String, Object>> queryJgccmx(Date rdate){
		try {
			return db.queryForList(SQL_QRY_JGCCMX, new Object[] {rdate}, new int[] {Types.DATE});
		}catch(Exception e) {
			logger.error("", e);
			return new ArrayList<>();
		}
	}
	
	private void updateJgccmxData(Map<String, Object> item, Date prevRDate) {
		Integer id = (Integer) item.get("id");
		String SCode = (String) item.get("SCode");
		String SHCode = (String) item.get("SHCode");
		
		Map<String, Object> prevItem = queryPrevJgccmx(prevRDate, SHCode, SCode);
		if(prevItem==null || prevItem.isEmpty()) {
			return;
		}
		updateJgccmxPrevData(id, prevRDate, prevItem);
	}

	/***
	 * 将机构持股明细，补上上期持股数据，便于分析
	 * @param rdate
	 */
	private static final String SQL_QRY_JGCCMX_PREV="SELECT ShareHDNum,Vposition FROM jgccmx where rdate=? and shcode=? and scode=?";
	private Map<String, Object> queryPrevJgccmx(Date rdate,String SHCode, String SCode){
		try {
			return db.queryForMap(SQL_QRY_JGCCMX_PREV, new Object[] {rdate, SHCode, SCode}, 
					new int[] {Types.DATE, Types.VARCHAR, Types.VARCHAR});
		}catch(EmptyResultDataAccessException e) {
			return null;
		}catch(Exception e) {
			logger.error("", e);
			return null;
		}
	}
	
	private static final String SQL_UPD_JGCCMX_PREV="UPDATE jgccmx set PrevRDate=?, PrevHDNum=?,"
	        +" PrevVPosition=?, ChangeHDNum=ShareHDNum-? , ChangeValue=Vposition-? "
			+" where id=?";
	private void updateJgccmxPrevData(Integer id, Date prevRDate, Map<String, Object> prevItem) {
		try {
			double ShareHDNum = ((BigDecimal)prevItem.get("ShareHDNum")).doubleValue();
			double Vposition = ((BigDecimal)prevItem.get("Vposition")).doubleValue();
			
			db.update( SQL_UPD_JGCCMX_PREV, 
					new Object[] {prevRDate, ShareHDNum, Vposition, ShareHDNum, Vposition, id}, 
					new int[] {Types.DATE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.INTEGER});
		}catch(Exception e) {
			logger.error("", e);
		}
	}

		
	
}
