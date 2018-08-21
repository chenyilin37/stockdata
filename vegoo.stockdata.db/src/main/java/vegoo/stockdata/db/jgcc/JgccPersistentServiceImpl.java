package vegoo.stockdata.db.jgcc;

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
import vegoo.stockdata.db.JgccPersistentService;
import vegoo.stockdata.db.base.PersistentServiceImpl;

@Component (immediate = true)
public class JgccPersistentServiceImpl extends PersistentServiceImpl implements JgccPersistentService, ManagedService {
	private static final Logger logger = LoggerFactory.getLogger(JgccPersistentServiceImpl.class);

	
    @Reference private RedisService redis;

    @Reference private JdbcService db;


	
	@Override
	public void updated(Dictionary<String, ?> properties) throws ConfigurationException {
		// TODO Auto-generated method stub
		
	}

	
    private static String SQL_EXIST_JGCC = "select SCode from jgcc where SCode=? and RDate=? and lx=?";
    public boolean existJgcc(String stkcode, Date rdate, String jglx) {
    	try {
    		String val = db.queryForObject(SQL_EXIST_JGCC, new Object[] {stkcode, rdate, jglx},
    				new int[] {Types.VARCHAR,Types.DATE, Types.VARCHAR}, String.class);
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
			db.update(SQL_INS_JGCC, new Object[] {sCODE,rDATE,jglx,cOUNT,cGChange,shareHDNum,vPosition,tabRate,lTZB,shareHDNumChange,rateChanges}, 
					  JGCC_FLD_Types);
		}catch(Exception e) {
			logger.error("",e);
		}
		
		uodateCalculatedFields(sCODE, rDATE, jglx);

	}

    private static String SQL_UPD_JGCC = "update jgcc set closeprice=round(vPosition/ShareHDNum, 2), ChangeValue=round(vPosition*ShareHDNumChange/ShareHDNum ,2) where SCode=? and RDate=? and lx=? and ShareHDNum<>0";
	private void uodateCalculatedFields(String stkcode, Date rdate, String jglx) {
		try {
			db.update(SQL_UPD_JGCC, new Object[] {stkcode, rdate, jglx},
					new int[] {Types.VARCHAR,Types.DATE, Types.VARCHAR});
		}catch(Exception e) {
			logger.error("",e);
		}
	}
	
}
