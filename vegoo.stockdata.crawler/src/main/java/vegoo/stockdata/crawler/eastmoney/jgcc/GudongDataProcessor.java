package vegoo.stockdata.crawler.eastmoney.jgcc;

import java.sql.Types;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.EmptyResultDataAccessException;

import com.google.common.base.Strings;

import vegoo.jdbcservice.JdbcService;

public class GudongDataProcessor {
	private static final Logger logger = LoggerFactory.getLogger(GudongDataProcessor.class);
	
    private JdbcService db;	
	
    public GudongDataProcessor(JdbcService db){
		this.db = db;
	}

   //public void saveGudongData(String SHCode, String SHName, String gdlx) {
   //	saveGudongData(SHCode, SHName, gdlx, null);
   //}
    
    //public void saveGudongData(String SHCode, String SHName, String gdlx, String lxdm) {
    //	saveGudongData(SHCode, SHName, gdlx,lxdm, null,null);
    	
    //}
 
	public void saveGudongData(String SHCode, String SHName, String gdlx, String lxdm, String indtCode, String indtName) {
		if(Strings.isNullOrEmpty(SHCode) || Strings.isNullOrEmpty(SHName)) {
			return;
		}
		
		if(existGudong(SHCode)) {
			return;
		}
		saveGudong(SHCode, SHName, gdlx, lxdm, indtCode, indtName);
		
		//if(Strings.isNullOrEmpty(indtCode)) {
		//	return;
		//}
		
		//saveFamilyData(indtCode, instSName);
		//saveMemberData(indtCode, SHCode);
	}

    
	private static final String SQL_existJigou = "select SHCode from gudong where SHCode=?";
	private boolean existGudong(String sHCode) {
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
	private void saveGudong(String SHCode, String SHName, String gdlx, String lxdm, String indtCode, String indtName) {
		try {
			db.update(SQL_addJigou2, new Object[] {SHCode, SHName, gdlx, lxdm, indtCode, indtName},
						new int[] {Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR});
		}catch(Exception e) {
			logger.error("",e);
		}
	}
    
	/*
	private void saveFamilyData(String indtCode, String instSName) {
		if(existFamily(indtCode)) {
			return;
		}
		insertFamily(indtCode, instSName);
	}

	private static final String SQL_existFamily = "select indtCode from gudongfamily where indtCode=?";
	private boolean existFamily(String indtCode) {
		try {
		  String result = db.queryForObject(SQL_existFamily, new Object[] {indtCode}, new int[]{Types.VARCHAR}, String.class);
		  return (result != null);
		}catch(EmptyResultDataAccessException e) {
			return false;
		}catch(Exception e) {
    		logger.error("", e);
			return false;
		}
	}
	
	private static final String SQL_INS_FAMILY = "insert into gudongfamily(indtCode, indtName) values(?,?)";
	private void insertFamily(String indtCode, String instSName) {
		try {
		   db.update(SQL_INS_FAMILY, new Object[] {indtCode, instSName},
					new int[] {Types.VARCHAR, Types.VARCHAR});
		}catch(Exception e) {
			logger.error("",e);
		}
	}
	private void saveMemberData(String indtCode, String sHCode) {
		if(existMember(indtCode, sHCode)) {
			return;
		}
		insertMember(indtCode, sHCode);
	}

	private static final String SQL_existMember = "select indtCode from gudongmember where indtCode=? and shCode=?";
	private boolean existMember(String indtCode, String sHCode) {
		try {
			  String result = db.queryForObject(SQL_existMember, new Object[] {indtCode, sHCode}, new int[] {Types.VARCHAR, Types.VARCHAR}, String.class);
			  return (result != null);
		}catch(EmptyResultDataAccessException e) {
			return false;
			}catch(Exception e) {
	    		logger.error("", e);
				return false;
			}
	}

	private static final String SQL_INS_Member = "insert into gudongmember(indtCode, shCode) values(?,?)";
	private void insertMember(String indtCode, String sHCode) {
		try {
			db.update(SQL_INS_Member, new Object[] {indtCode, sHCode},
						new int[] {Types.VARCHAR, Types.VARCHAR});
		}catch(Exception e) {
			logger.error("",e);
		}
	}
*/


}
