package vegoo.stockdata.db.jgcc;

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

import vegoo.jdbcservice.JdbcService;
import vegoo.redis.RedisService;
import vegoo.stockdata.db.HQPersistentService;
import vegoo.stockdata.db.JgccPersistentService;
import vegoo.stockdata.db.base.PersistentServiceImpl;

@Component (immediate = true)
public class JgccPersistentServiceImpl extends PersistentServiceImpl implements JgccPersistentService, ManagedService {
	private static final Logger logger = LoggerFactory.getLogger(JgccPersistentServiceImpl.class);
	
    @Reference private RedisService redis;

    @Reference private JdbcService db;

    @Reference
    private HQPersistentService dbKDayData; // (policy=ReferencePolicy.DYNAMIC)

	
	@Override
	public void updated(Dictionary<String, ?> properties) throws ConfigurationException {
		// TODO Auto-generated method stub
		
	}
	
    private static String SQL_EXIST_JGCC = "select SCode from jgcc where SCode=? and RDate=? and lx=?";
    public boolean existJgcc(String stkcode, Date rdate, int jglx) {
    	try {
    		String val = db.queryForObject(SQL_EXIST_JGCC, new Object[] {stkcode, rdate, jglx},
    				new int[] {Types.VARCHAR,Types.DATE, Types.INTEGER}, String.class);
		    return  val != null;
		}catch(EmptyResultDataAccessException e) {
			return false;
    	}catch(Exception e) {
    		logger.error("",e);
    		return false;
    	}
    }
	
	private static final String SQL_INS_JGCC = "insert into jgcc(SCODE,RDATE,LX,COUNT,CGChange,ShareHDNum,VPosition,TabRate,LTZB,ShareHDNumChange,RateChange) "
			                                            + "values (?,?,?,?,?,?,?,?,?,?,?)";
	//private static final String[] JGCC_FLDS_DB = {F_SCODE,F_RDATE,F_JGLX, F_COUNT, F_CGChange, F_ShareHDNum,F_VPosition,F_TabRate,F_LTZB,F_ShareHDNumChange,F_RateChange}; //LTZBChange
	//private static final String[] JGCC_FLDS_JS = {F_SCODE,F_RDATE,F_JGLX, F_COUNT, F_CGChange, F_ShareHDNum,F_VPosition,F_TabRate,F_LTZB,F_ShareHDNumChange,F_RateChange}; //LTZBChange
	private static final int[] JGCC_FLD_Types  = {Types.VARCHAR,Types.DATE, Types.VARCHAR,Types.DOUBLE,Types.VARCHAR,Types.DOUBLE,Types.DOUBLE,Types.DOUBLE,Types.DOUBLE,Types.DOUBLE,Types.DOUBLE};
	
	public void insertJgcc(String sCODE, Date rDATE,String jglx,double cOUNT,
			String cGChange, double shareHDNum,double vPosition,double tabRate,
			double lTZB,double shareHDNumChange,double rateChanges) {

		try {
			db.update( SQL_INS_JGCC, 
					new Object[] {sCODE,rDATE,jglx,cOUNT,cGChange,shareHDNum,vPosition,tabRate,lTZB,shareHDNumChange,rateChanges}, 
					  JGCC_FLD_Types);
		}catch(Exception e) {
			logger.error("",e);
		}
		
		uodateCalculatedFields(sCODE, rDATE, jglx);

	}

    private static String SQL_UPD_JGCC = "update jgcc set closeprice=round(vPosition/ShareHDNum, 2), ChangeValue=round(vPosition*ShareHDNumChange/ShareHDNum ,2) where SCode=? and RDate=? and lx=? and ShareHDNum<>0";
	private void uodateCalculatedFields(String stkcode, Date rdate, String jglx) {
		try {
			db.update( SQL_UPD_JGCC, new Object[] {stkcode, rdate, jglx},
					new int[] {Types.VARCHAR,Types.DATE, Types.VARCHAR});
		}catch(Exception e) {
			logger.error("",e);
		}
	}
	
	
	@Override
	public void settleJgcc() {
		List<Date> reportDates = queryReportDateOfJgccNullTransDate();
		for(Date rptDate:reportDates) {
			processJgcc(rptDate);
		}
	}
	
	private static final String QRY_NULL_TRNSDATE_JGCC = "select distinct RDate from jgcc where EndTradeDate is null order by RDate desc";
	private List<Date> queryReportDateOfJgccNullTransDate() {
		try {
			return db.queryForList(QRY_NULL_TRNSDATE_JGCC, Date.class);
		}catch(Exception e) {
			logger.error("", e);
			return new ArrayList<>();
		}
	}

	private int processJgcc(Date rptDate) {
		List<Map<String, Object>> items = queryNullTransDateOfJgcc(rptDate);
		if(items==null||items.isEmpty()) {
			return 0;
		}
		
		for(Map<String, Object> item:items) {
		  updateNullTransDateOfJgcc(rptDate, item);
		}
		return items.size();
		
	}

	private static final String QRY_JGCC_NULLTRNS="select id, scode from jgcc where rdate=? and EndTradeDate is null";
    private List<Map<String, Object>> queryNullTransDateOfJgcc(Date rptDate){
    	try {
    		return db.queryForList(QRY_JGCC_NULLTRNS, new Object[] {rptDate}, 
    				new int[] {Types.DATE});
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
			db.update( UPD_JGCC_TRNSDATE, new Object[] {endTradeDate, id},
					new int[] {Types.DATE, Types.INTEGER});
		}catch(EmptyResultDataAccessException e) {
			return;
		}catch(Exception e) {
			logger.error("",e);
			return;
		}
	}
	
}
