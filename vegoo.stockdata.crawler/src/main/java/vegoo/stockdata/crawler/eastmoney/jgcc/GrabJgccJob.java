package vegoo.stockdata.crawler.eastmoney.jgcc;

import java.beans.PropertyVetoException;
import java.sql.Types;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.sql.DataSource;

import org.apache.commons.lang3.StringUtils;
import org.apache.karaf.scheduler.Job;
import org.apache.karaf.scheduler.JobContext;
import org.apache.karaf.scheduler.Scheduler;
import org.osgi.service.cm.ConfigurationException;
import org.osgi.service.cm.ManagedService;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.EmptyResultDataAccessException;

import com.google.common.base.Strings;

import vegoo.commons.JsonUtil;
import vegoo.jdbcservice.JdbcService;
import vegoo.stockdata.crawler.eastmoney.ReportDataGrabJob;

/**
 * 抓取机构持仓
 * @author cyl
 *
 */

@Component (
		service = {Job.class,  ManagedService.class}, 
		immediate = true, 
		configurationPid = "stockdata.grab.jgcc",
		property = {
		    Scheduler.PROPERTY_SCHEDULER_EXPRESSION + "= 0 0 1,8,18 * * ?",   // 静态信息，每天7，8，18抓三次
		    // Scheduler.PROPERTY_SCHEDULER_CONCURRENT + "= false"
		} 
	) 
public class GrabJgccJob extends ReportDataGrabJob implements Job, ManagedService {
	private static final Logger logger = LoggerFactory.getLogger(GrabJgccJob.class);
/*
#       {
#       	"Message":""
#       	"Status":0
#       	"Data":[
#       		{
#       			"TableName":"RptMainDataMap"
#       			"TotalPage":511
#       			"ConsumeMSecond":1193
#       			"SplitSymbol":"|"
#       			"FieldName":"SCode,SName,RDate,LXDM,LX,Count,CGChange,ShareHDNum,VPosition,TabRate,LTZB,ShareHDNumChange,RateChange"
#       			"Data":[
#       				"600887|伊利股份|2018-06-30|基金|1|518|增持|784394737|21884613162.3|12.90442854|13.00094057|23896629|3.14223385286844"
#       				"000333|美的集团|2018-06-30|基金|1|507|增持|431672071|22541915547.62|6.51550737|6.55696217000001|21427708|5.22315720399064"
#       			    "600036|招商银行|2018-06-30|基金|1|454|减持|685813530|18132909733.2|2.71934072|3.32452081|-53778029|-7.27131459865674"
#       			]
#       		}
#       	]
#       }
# 
 */
	
	// 机构持仓jgcc的字段名常量，根据上述抓去数据包中FieldName定义，便于与数据库转换
	private static final String F_SCODE = "SCode".toLowerCase();
	private static final String F_RDATE = "RDate".toLowerCase();
	private static final String F_JGLX = "LX".toLowerCase();
	private static final String F_SNAME = "SName".toLowerCase();
	private static final String F_LXDM = "LXDM".toLowerCase();
	private static final String F_COUNT = "Count".toLowerCase();
	private static final String F_CGChange = "CGChange".toLowerCase();
	private static final String F_ShareHDNum = "ShareHDNum".toLowerCase();
	private static final String F_VPosition = "VPosition".toLowerCase();
	private static final String F_TabRate = "TabRate".toLowerCase();
	private static final String F_LTZB = "LTZB".toLowerCase();
	private static final String F_ShareHDNumChange = "ShareHDNumChange".toLowerCase();
	private static final String F_RateChange = "RateChange".toLowerCase();

	/*
	 * {
        		"Message":"",
        		"Status":0,
       			"Data":[{
        				"TableName":"ZLHoldDetailsMap",
        				"TotalPage":1,
        				"ConsumeMSecond":113,
        				"SplitSymbol":"|",
        				"FieldName":"SCode,SName,RDate,SHCode,SHName,IndtCode,InstSName,TypeCode,Type,ShareHDNum,Vposition,TabRate,TabProRate",
        				"Data":[
        						"002462.SZ|嘉事堂|2018-06-30|630001|华商领先企业混合|80053204|华商基金管理有限公司|1|基金|2754297|53433361.8|1.10|1.10",
        						"002462.SZ|嘉事堂|2018-06-30|450005|国富强化收益债券A|80044515|国海富兰克林基金管理有限公司|1|基金|269300|5224420|0.11|0.11",
                   ... ...
        				]
        		}]
        }
	 */
	// 机构持仓明细jgccmx的字段名常量
	private static final String F_MX_SCode = "SCode".toLowerCase();
	private static final String F_MX_SName = "SName".toLowerCase();
	private static final String F_MX_RDate = "RDate".toLowerCase();
	private static final String F_MX_SHCode = "SHCode".toLowerCase();
	private static final String F_MX_SHName = "SHName".toLowerCase();
	private static final String F_MX_IndtCode = "IndtCode".toLowerCase();
	private static final String F_MX_InstSName = "InstSName".toLowerCase();
	private static final String F_MX_TypeCode = "TypeCode".toLowerCase();
	private static final String F_MX_Type = "Type".toLowerCase();
	private static final String F_MX_ShareHDNum = "ShareHDNum".toLowerCase();
	private static final String F_MX_Vposition = "Vposition".toLowerCase();
	private static final String F_MX_TabRate = "TabRate".toLowerCase();
	private static final String F_MX_TabProRate = "TabProRate".toLowerCase();
	
	private static final String PN_REPORT_DATES = "preload-reports";
	private static final String PN_URL_JGCC   = "url-jgcc";
	private static final String PN_URL_JGCCMX   = "url-jgccmx";

	//private static final String TAG_REPORTDATE = "<REPORT_DATE>";
	//private static final String TAG_PAGENO     = "<PAGE_NO>";
	//private static final String TAG_STOCKCODE  = "<STOCK_FCODE>"; //带市场后缀，如：000001.sz
	private static final String TAG_JGLX       = "<JGLX>";

    @Reference
    private JdbcService db;
	
	private String jgccURL ;
	private String jgccmxURL ;
	
	@Override
	public void updated(Dictionary<String, ?> properties) throws ConfigurationException {
		this.jgccURL = (String) properties.get(PN_URL_JGCC);
		this.jgccmxURL = (String) properties.get(PN_URL_JGCCMX);

		String reports   = (String) properties.get(PN_REPORT_DATES);
		
		logger.info("{} = {}", PN_URL_JGCC, jgccURL);
		logger.info("{} = {}", PN_REPORT_DATES, reports);
		
		if(!Strings.isNullOrEmpty(reports) && !Strings.isNullOrEmpty(jgccURL) 
				&& !Strings.isNullOrEmpty(jgccmxURL)) {
			preloadJgccData(reports.trim());
		}
	}

	private void preloadJgccData(String data) {
		String[] reports = split(data, ",");
		
		for(String report : reports) {
			if(Strings.isNullOrEmpty(report)) {
				continue;
			}
			
			asyncExecute(new Runnable() {

				@Override
				public void run() {
					grabJgccData(report.trim());
				}});
		}
	}
	
	@Override
	protected void executeJob(JobContext context) {
		String latestReportDate = getLatestReportDateAsString();
		
		grabJgccData(latestReportDate);
	}


	private void grabJgccData(String reportDate) {
		if(Strings.isNullOrEmpty(jgccURL)) {
			logger.error("没有在配置文件中设置{}参数！", PN_URL_JGCC);
			return;
		}
		if(Strings.isNullOrEmpty(jgccmxURL)) {
			logger.error("没有在配置文件中设置{}参数！", PN_URL_JGCCMX);
			return;
		}

		
		// 用于缓存机构持仓个股，以便获取机构持仓明细
		Set<String> jgccStocks = new HashSet<>();

		
		String url = jgccURL.replaceAll(TAG_REPORTDATE, reportDate);
		
		// 机构类型代码：1-基金 2-QFII 3-社保 4-券商 5-保险 6-信托
		for(int i=1; i<=6; ++i) {
			grabJgccDataByLx(i, url, jgccStocks, db);
		}
		
		// 抓去机构持仓明细
		grabJgccmxData(reportDate, jgccStocks, db);
	}


	private void grabJgccDataByLx(int jglx, String urlPattern, Set<String> jgccStocks, JdbcService db) {
		String url = urlPattern.replaceAll(TAG_JGLX, String.valueOf(jglx) );
		
		int page = 0;
		while(grabJgccDataByPage(++page, url, jgccStocks, db) > page) ;
	}
	
	private int grabJgccDataByPage(int page, String urlPattern, Set<String> jgccStocks, JdbcService db) {
		String url = urlPattern.replaceAll(TAG_PAGENO, String.valueOf(page));
		
		JgccListDto listDto = requestData(url, JgccListDto.class, "获取机构持仓");

		if(listDto == null) {
			return 0;
		}
			
		JgccListDataDto[] dataDtos = listDto.getData();
		if(dataDtos==null || dataDtos.length==0) {
			return 0;
		}
		
		JgccListDataDto dataDto = dataDtos[0];
		
		processJgccData(dataDto, jgccStocks, db);
		
		return dataDto.getTotalPage();
	}

	private void processJgccData(JgccListDataDto dataDto, Set<String> jgccStocks, JdbcService db) {
		String splitSymbol = dataDto.getSplitSymbol();
		// 通配符转义
		if("|".equals(splitSymbol)) {  
			splitSymbol = "\\"+splitSymbol;
		}
		
		String[] fieldNames = split(dataDto.getFieldName(), ",") ;
		
		List<String> data = dataDto.getData();
		
		for(String item : data) {
			if(Strings.isNullOrEmpty(item)) {
				continue;
			}
		    processJgccData(splitSymbol, fieldNames, item.trim(), jgccStocks, db);
		}
	}

	private void processJgccData(String splitSymbol, String[] fieldNames, String data, Set<String> jgccStocks, JdbcService db) {
		String[] values = split(data, splitSymbol);
		
		if(fieldNames.length != values.length) {
			logger.error("字段数为{}，与数值{}不相等: {}", fieldNames.length, values.length, data);
			return;
		}

		Map<String, String> fieldValues = new HashMap<>();
		for(int i=0;i<fieldNames.length; ++i) {
			fieldValues.put(fieldNames[i].trim().toLowerCase(), values[i].trim());
		}
		
		String scode = fieldValues.get(F_SCODE);
		String rdate = fieldValues.get(F_RDATE);
		String jglx  = fieldValues.get(F_JGLX);
		
		if(scode==null || rdate==null || jglx==null) {
			logger.error("机构持股数据项为空：{}={}, {}={}, {}={};", F_SCODE,scode, F_RDATE,rdate, F_JGLX, jglx);
			return;
		}
		
		Date reportDate = null;
		try {
			reportDate = JsonUtil.parseDate(rdate);
		} catch (ParseException e) {
			logger.error("invalid report date:{}", rdate, e);
			return;
		}
		
		jgccStocks.add(scode.trim());
		
		if(existJgccData(scode, reportDate, jglx, db)) {
			return;
		}
		
		saveJgccData(fieldValues, db);
		uodateCalculatedFields(scode, reportDate, jglx, db);
	}

    private static String SQL_EXIST_JGCC = "select SCode from jgcc where SCode=? and RDate=? and lx=?";
    private boolean existJgccData(String stkcode, Date rdate, String jglx, JdbcService db) {
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
	
	private static final String SQL_ADD_JGCC = "insert into jgcc(%s) values (%s)";
	private static final String[] JGCC_FLDS_DB = {F_SCODE,F_RDATE,F_JGLX, F_COUNT, F_CGChange, F_ShareHDNum,F_VPosition,F_TabRate,F_LTZB,F_ShareHDNumChange,F_RateChange}; //LTZBChange
	private static final String[] JGCC_FLDS_JS = {F_SCODE,F_RDATE,F_JGLX, F_COUNT, F_CGChange, F_ShareHDNum,F_VPosition,F_TabRate,F_LTZB,F_ShareHDNumChange,F_RateChange}; //LTZBChange
	private static final int[] JGCC_FLD_Types  = {Types.VARCHAR,Types.VARCHAR, Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,Types.VARCHAR};
	
	private void saveJgccData(Map<String, String> fieldValues, JdbcService db) {
		String fields = StringUtils.join(JGCC_FLDS_DB, ",");
		
		StringBuffer sbParams = new StringBuffer();
		
		List<Object> prmValues = new ArrayList<>();
		
		for(int i=0; i<JGCC_FLDS_JS.length; ++i) {
			String fld = JGCC_FLDS_JS[i];
			if(i>0) {
				sbParams.append(",");
			}
			
			sbParams.append("?");
			String val = fieldValues.get(fld);
			if(F_COUNT.equalsIgnoreCase(fld)
							||F_ShareHDNum.equalsIgnoreCase(fld)
							||F_VPosition.equalsIgnoreCase(fld)
						||F_TabRate.equalsIgnoreCase(fld)
						||F_LTZB.equalsIgnoreCase(fld)
						||F_ShareHDNumChange.equalsIgnoreCase(fld)
					||F_RateChange.equalsIgnoreCase(fld)) {
				try {
				   Double.parseDouble(val);
				}catch(Exception e) {
					val = "0";
				}
			}
			prmValues.add(val);
		}
		
		String sql = String.format(SQL_ADD_JGCC, fields, sbParams);
		
		try {
			db.update(sql, prmValues.toArray(), JGCC_FLD_Types);
			
		}catch(Exception e) {
			logger.error("",e);
		}
	}

    private static String SQL_UPD_JGCC = "update jgcc set closeprice=round(vPosition/ShareHDNum, 2), ChangeValue=round(vPosition*ShareHDNumChange/ShareHDNum ,2) where SCode=? and RDate=? and lx=? and ShareHDNum<>0";
	private void uodateCalculatedFields(String stkcode, Date rdate, String jglx, JdbcService db) {
		try {
			db.update(SQL_UPD_JGCC, new Object[] {stkcode, rdate, jglx},
					new int[] {Types.VARCHAR,Types.DATE, Types.VARCHAR});
		}catch(Exception e) {
			logger.error("",e);
		}
	}
	
	

	/******************************************************************
	 * 抓去机构持仓明细
	 *******************************************************************/
	private void grabJgccmxData(String reportDate, Set<String> stocks, JdbcService db) {
		String url = jgccmxURL.replaceAll(TAG_REPORTDATE, reportDate);

		for(String stkcode : stocks) {
			String stkFcode = getStockCodeWithMarketFix(stkcode);
			grabJgccmxData(url, stkFcode, db);
		}
		
	}

	private void grabJgccmxData(String urlPattern, String stkFcode, JdbcService db) {
		String url = urlPattern.replaceAll(TAG_STOCKCODE, stkFcode );
		
		int page = 0;
		while(grabJgccmxDataByPage(++page, url, db) > page) ;
	}

	private int grabJgccmxDataByPage(int page, String urlPattern, JdbcService db) {
		String url = urlPattern.replaceAll(TAG_PAGENO, String.valueOf(page));
		
		JgccListDto listDto = requestData(url, JgccListDto.class, "获取机构持仓明细");

		if(listDto == null) {
			return 0;
		}
			
		JgccListDataDto[] dataDtos = listDto.getData();
		if(dataDtos==null || dataDtos.length==0) {
			return 0;
		}
		
		JgccListDataDto dataDto = dataDtos[0];
		
		processJgccmxData(dataDto, db);
		
		return dataDto.getTotalPage();
	}

	private void processJgccmxData(JgccListDataDto dataDto, JdbcService db) {
		String splitSymbol = dataDto.getSplitSymbol();
		// 通配符转义
		if("|".equals(splitSymbol)) {  
			splitSymbol = "\\"+splitSymbol;
		}
		
		String[] fieldNames = split(dataDto.getFieldName(), ",") ;
		
		List<String> data = dataDto.getData();
		
		for(String item : data) {
			if(Strings.isNullOrEmpty(item)) {
				continue;
			}
		    processJgccmxData(splitSymbol, fieldNames, item.trim(), db);
		}
		
	}

	private void processJgccmxData(String splitSymbol, String[] fieldNames, String data, JdbcService db) {
		String[] values = split(data, splitSymbol);
		
		if(fieldNames.length != values.length) {
			logger.error("字段数为{}，与数值{}不相等: {}", fieldNames.length, values.length, data);
			return;
		}

		Map<String, String> fieldValues = new HashMap<>();
		for(int i=0;i<fieldNames.length; ++i) {
			fieldValues.put(fieldNames[i].trim().toLowerCase(), values[i].trim());
		}
		
		String stkFCodes = fieldValues.get(F_MX_SCode);
		String[] stkcodeFields = split(stkFCodes, ".");
		if(stkcodeFields.length != 2) {
			logger.error("机构持股明细数据股票代码未带市场后缀，如:.sh/.sz,数据结构可能发生了变化：{}",stkFCodes );
		}
		
		String scode = stkcodeFields[0];
		String rdate = fieldValues.get(F_MX_RDate);
		String shcode = fieldValues.get(F_MX_SHCode);
		
		if(scode==null || rdate==null || shcode==null) {
			logger.error("机构持股数据项为空：{}={}, {}={}, {}={};", F_MX_SCode,scode, F_MX_RDate,rdate, F_MX_SHCode, shcode);
			return;
		}

		Date reportDate = null;
		try {
			reportDate = JsonUtil.parseDate(rdate);
		} catch (ParseException e) {
			logger.error("invalid report date:{}", rdate, e);
			return;
		}
		
		if(existJgccmxData(scode, reportDate, shcode, db)) {
			return;
		}
		
		fieldValues.put(F_MX_SCode, scode);  // 用000001代替000001.sz
		
		saveJgccmxData(scode, reportDate, shcode, fieldValues, db);	
		
		saveJigouData(fieldValues, db);
	}


	private static String SQL_EXIST_JGCCMX = "select SCode from jgccmx where SCode=? and RDate=? and SHCode=?";
    private boolean existJgccmxData(String scode, Date rdate, String shcode, JdbcService db) {
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
	private static final String SQL_ADD_JGCCMX = "insert into jgccmx(%s) values (%s)";
	private static final String[] MX_FLDS_DB = {"SCode","RDate","SHCode","IndtCode","TypeCode","ShareHDNum","Vposition","TabRate","TabProRate"};
	private static final String[] MX_FLDS_JSON = {F_MX_SCode,F_MX_RDate,F_MX_SHCode,F_MX_IndtCode,F_MX_TypeCode,F_MX_ShareHDNum,F_MX_Vposition,F_MX_TabRate,F_MX_TabProRate}; 
	private static final int[] MX_FLD_Types  = {Types.VARCHAR,Types.VARCHAR, Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,Types.VARCHAR};
	
	private void saveJgccmxData(String stkcode, Date rdate, String jglx, Map<String, String> fieldValues, JdbcService db) {
		String db_fields = StringUtils.join(MX_FLDS_DB, ",");
		
		StringBuffer sbParams = new StringBuffer();
		
		
		for(int i=0; i<MX_FLDS_DB.length; ++i) {
			if(i>0) {
				sbParams.append(",");
			}
			
			sbParams.append("?");
		}
		
		String sql = String.format(SQL_ADD_JGCCMX, db_fields, sbParams);

		List<Object> prmValues = new ArrayList<>();
		
		for(String fld: MX_FLDS_JSON) {
			String val = fieldValues.get(fld);
			if(F_MX_ShareHDNum.equalsIgnoreCase(fld) || F_MX_Vposition.equalsIgnoreCase(fld)
				||F_MX_TabRate.equalsIgnoreCase(fld) || F_MX_TabProRate.equalsIgnoreCase(fld) ) {
				try {
				   Double.parseDouble(val);
				}catch(Exception e) {
					val = "0";
				}
			}
			prmValues.add(val);
		}
		
		try {
			db.update(sql, prmValues.toArray(), MX_FLD_Types);
		}catch(Exception e) {
			logger.error("",e);
		}
	}

	/*********************************
	 * 保存股东信息
	 * @param fieldValues
	 */
    private void saveJigouData(Map<String, String> fieldValues, JdbcService db) {
    	String SHCode= fieldValues.get(F_MX_SHCode); 
    	String SHName= fieldValues.get(F_MX_SHName); 
    	String gdlx = fieldValues.get(F_MX_Type);
    	String lxdm = fieldValues.get(F_MX_TypeCode);
    	String indtCode = fieldValues.get(F_MX_IndtCode); 
    	String instSName= fieldValues.get(F_MX_InstSName);
    	
    	GudongDataProcessor processor = new GudongDataProcessor(db);
    	processor.saveGudongData(SHCode, SHName, gdlx, lxdm, indtCode, instSName);

	}

	
}
