package vegoo.stockdata.db.jgcgmx;

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
			db.update(SQL_INS_JGCCMX, 
					  new Object[] {SCode,RDate,SHCode,IndtCode,TypeCode,ShareHDNum,Vposition,TabRate,TabProRate}, 
					  MX_FLD_Types);
		}catch(Exception e) {
			logger.error("",e);
		}
	}

	
	
}
