package vegoo.stockdata.db.sdltgd;

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
import vegoo.stockdata.db.SdltgdPersistentService;
import vegoo.stockdata.db.base.PersistentServiceImpl;

@Component (immediate = true)
public class SdltgdPersistentServiceImpl  extends PersistentServiceImpl implements SdltgdPersistentService, ManagedService{
	private static final Logger logger = LoggerFactory.getLogger(SdltgdPersistentServiceImpl.class);

    @Reference private RedisService redis;

    @Reference private JdbcService db;

	@Override
	public void updated(Dictionary<String, ?> properties) throws ConfigurationException {
		// TODO Auto-generated method stub
		
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
	         +"RANK,SCODE,RDATE,SHAREHDNUM,LTAG,ZB,NDATE,BZ,BDBL,SHAREHDCODE,SHAREHDRATIO,BDSUM)"
			 + " values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
	public void insertSdltgd(String companycode, String sharehdname, String sharehdtype, String sharestype,
			double rank, String scode, Date rdate, double sharehdnum, double ltag, double zb, Date ndate, String bz,
			double bdbl, String sharehdcode, double sharehdratio, double bdsum) {
		db.update(SQL_ADD_SDLTGD, new Object[] {companycode, sharehdname, sharehdtype, sharestype,
				 rank, scode,  rdate,  sharehdnum,  ltag,  zb,  ndate,  bz,
				 bdbl, sharehdcode,  sharehdratio,  bdsum},
					new int[] {Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,Types.DOUBLE,
							Types.VARCHAR,Types.DATE,Types.DOUBLE,Types.DOUBLE,Types.DOUBLE,
							Types.DATE,Types.VARCHAR,Types.DOUBLE,Types.VARCHAR,Types.DOUBLE,Types.DOUBLE});
	}
		
}
