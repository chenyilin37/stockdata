package vegoo.stockdata.db.gudong;

import java.sql.Types;
import java.util.Dictionary;

import org.osgi.service.cm.ConfigurationException;
import org.osgi.service.cm.ManagedService;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.EmptyResultDataAccessException;

import com.google.common.base.Strings;

import vegoo.jdbcservice.JdbcService;
import vegoo.redis.RedisService;
import vegoo.stockdata.db.GudongPersistentService;
import vegoo.stockdata.db.base.PersistentServiceImpl;

@Component (immediate = true)
public class GudongPersistentServiceImpl  extends PersistentServiceImpl implements GudongPersistentService, ManagedService {
	private static final Logger logger = LoggerFactory.getLogger(GudongPersistentServiceImpl.class);

    @Reference private RedisService redis;

    @Reference private JdbcService db;

	@Override
	public void updated(Dictionary<String, ?> properties) throws ConfigurationException {
		/* ！！！本函数内不要做需要长时间才能完成的工作，否则，会影响其他BUNDLE的初始化！！！  */
		
	}
	
	
	public void saveGudong(String SHCode,String SHName,String gdlx) {
		
		saveGudong(SHCode, SHName, gdlx, null, null, null);
	}
	
	public void saveGudong(String SHCode, String SHName, String gdlx, String lxdm, String indtCode, String indtName) {
		if(Strings.isNullOrEmpty(SHCode) || Strings.isNullOrEmpty(SHName)) {
			return;
		}
		
		if(existGudong(SHCode)) {
			return;
		}
		
		inserteGudong(SHCode, SHName, gdlx, lxdm, indtCode, indtName);
		
		//if(Strings.isNullOrEmpty(indtCode)) {
		//	return;
		//}
		
		//saveFamilyData(indtCode, instSName);
		//saveMemberData(indtCode, SHCode);
	}

    
	private static final String SQL_existJigou = "select SHCode from gudong where SHCode=?";
	@Override
	public boolean existGudong(String sHCode) {
		try {
		  String result = db.queryForObject(SQL_existJigou, new Object[] {sHCode}, 
				  new int[]{Types.VARCHAR}, String.class);
		  return (result != null);
		}catch(EmptyResultDataAccessException e) {
			return false;
		}catch(Exception e) {
    		logger.error("", e);
			return false;
		}
	}

	// private static final String SQL_addJigou1 = "insert into gudong(sHCode, SHName, gdlx) values(?,?,?)";
	private static final String SQL_addJigou2 = "insert into gudong(sHCode, SHName, gdlx, lxdm, indtCode, indtName) values(?,?,?,?,?,?)";
	@Override
	public void inserteGudong(String SHCode, String SHName, String gdlx, String lxdm, String indtCode, String indtName) {
		try {
			db.update(SQL_addJigou2, 
					new Object[] {SHCode, SHName, gdlx, lxdm, indtCode, indtName},
						new int[] {Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR});
		}catch(Exception e) {
			logger.error("",e);
		}
	}

}
