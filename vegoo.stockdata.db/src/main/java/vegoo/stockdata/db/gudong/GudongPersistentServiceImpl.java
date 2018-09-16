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

import vegoo.commons.jdbc.JdbcService;
import vegoo.commons.redis.RedisService;
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
			if(Strings.isNullOrEmpty(lxdm)) {
				if(gdlx.equals("基金")) {
					lxdm = "1";
				}else if(gdlx.equals("QFII")) {
					lxdm = "2";
				}else if(gdlx.equals("社保")) {
					lxdm = "3";
				}else if(gdlx.equals("券商")) {
					lxdm = "4";
				}else if(gdlx.equals("保险")) {
					lxdm = "5";
				}else if(gdlx.equals("信托")) {
					lxdm = "6";
				}else if(gdlx.equals("基本养老基金")) {//（此编号及以下，自编，待确认）
					lxdm = "81";
				}else if(gdlx.equals("企业年金")) {//（此编号及以下，自编，待确认）
					lxdm = "82";
				}else if(gdlx.equals("金融")) {//（此编号及以下，自编，待确认）
					lxdm = "83";
				}else if(gdlx.equals("财务公司")) {//（此编号及以下，自编，待确认）
					lxdm = "84";
				}else if(gdlx.equals("投资公司")) {//（此编号及以下，自编，待确认）
					lxdm = "85";
				}else if(gdlx.equals("集合理财计划")) {//（此编号及以下，自编，待确认）
					lxdm = "86";
				}else if(gdlx.equals("高校")) {//（此编号及以下，自编，待确认）
					lxdm = "87";
				}else if(gdlx.equals("个人")) {//（此编号及以下，自编，待确认）
					lxdm = "88";
				}else if(gdlx.equals("其他")) {//（此编号及以下，自编，待确认）
					lxdm = "99";
				}else {
					lxdm = "0";  // 未定义，未知
				}
			}

			db.update(SQL_addJigou2, 
					new Object[] {SHCode, SHName, gdlx, lxdm, indtCode, indtName},
					new int[] {Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR});
		}catch(Exception e) {
			logger.error("",e);
		}
	}

}
