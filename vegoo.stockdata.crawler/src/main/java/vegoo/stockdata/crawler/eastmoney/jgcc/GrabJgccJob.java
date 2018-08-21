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
import vegoo.stockdata.db.GudongPersistentService;
import vegoo.stockdata.db.JgccPersistentService;
import vegoo.stockdata.db.JgccmxPersistentService;

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
    private JgccPersistentService dbJgcc;
    @Reference
    private JgccmxPersistentService dbJgccmx;
    @Reference
    private GudongPersistentService dbGudong;
	
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
			grabJgccDataByLx(i, url, jgccStocks);
		}
		
		// 抓去机构持仓明细
		grabJgccmxData(reportDate, jgccStocks);
	}


	private void grabJgccDataByLx(int jglx, String urlPattern, Set<String> jgccStocks) {
		String url = urlPattern.replaceAll(TAG_JGLX, String.valueOf(jglx) );
		
		int page = 0;
		while(grabJgccDataByPage(++page, url, jgccStocks) > page) ;
	}
	
	private int grabJgccDataByPage(int page, String urlPattern, Set<String> jgccStocks) {
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
		
		processJgccData(dataDto, jgccStocks);
		
		return dataDto.getTotalPage();
	}

	private void processJgccData(JgccListDataDto dataDto, Set<String> jgccStocks) {
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
		    processJgccData(splitSymbol, fieldNames, item.trim(), jgccStocks);
		}
	}

	private void processJgccData(String splitSymbol, String[] fieldNames, String data, Set<String> jgccStocks) {
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
		
		if(dbJgcc.existJgcc(scode, reportDate, jglx)) {
			return;
		}

		String cGChange = fieldValues.get(F_CGChange);
		
		double cOUNT = getFieldValue(fieldValues, F_COUNT);
		double shareHDNum = getFieldValue(fieldValues, F_ShareHDNum);
		double vPosition =  getFieldValue(fieldValues, F_VPosition);
		double tabRate = getFieldValue(fieldValues, F_TabRate);
		double lTZB = getFieldValue(fieldValues, F_LTZB);
		double shareHDNumChange = getFieldValue(fieldValues, F_ShareHDNumChange);
		double rateChanges = getFieldValue(fieldValues, F_RateChange);
		
		dbJgcc.insertJgcc(scode,reportDate,jglx,cOUNT,cGChange,shareHDNum,vPosition,tabRate,lTZB,shareHDNumChange,rateChanges);
	}

	

	private static double getFieldValue(Map<String, String> fieldValues, String fieldName) {
		try {
			return Double.parseDouble(fieldValues.get(fieldName));
		}catch(Exception e) {
			return 0;
		}
		
	}

	/******************************************************************
	 * 抓去机构持仓明细
	 *******************************************************************/
	private void grabJgccmxData(String reportDate, Set<String> stocks) {
		String url = jgccmxURL.replaceAll(TAG_REPORTDATE, reportDate);

		for(String stkcode : stocks) {
			String stkFcode = getStockCodeWithMarketFix(stkcode);
			grabJgccmxData(url, stkFcode);
		}
		
	}

	private void grabJgccmxData(String urlPattern, String stkFcode) {
		String url = urlPattern.replaceAll(TAG_STOCKCODE, stkFcode );
		
		int page = 0;
		while(grabJgccmxDataByPage(++page, url) > page) ;
	}

	private int grabJgccmxDataByPage(int page, String urlPattern) {
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
		
		processJgccmxData(dataDto);
		
		return dataDto.getTotalPage();
	}

	private void processJgccmxData(JgccListDataDto dataDto) {
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
		    processJgccmxData(splitSymbol, fieldNames, item.trim());
		}
		
	}

	private void processJgccmxData(String splitSymbol, String[] fieldNames, String data) {
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
		
		if(dbJgccmx.existJgccmx(scode, reportDate, shcode)) {
			return;
		}
		
		fieldValues.put(F_MX_SCode, scode);  // 用000001代替000001.sz
		
    	String indtCode = fieldValues.get(F_MX_IndtCode); 
    	String instSName= fieldValues.get(F_MX_InstSName);
    	String SHName= fieldValues.get(F_MX_SHName); 
    	String gdlx = fieldValues.get(F_MX_Type);
    	String lxdm = fieldValues.get(F_MX_TypeCode);

		double ShareHDNum = getFieldValue(fieldValues, F_MX_ShareHDNum);
		double Vposition = getFieldValue(fieldValues, F_MX_Vposition);
		double TabRate = getFieldValue(fieldValues, F_MX_TabRate);
		double TabProRate = getFieldValue(fieldValues, F_MX_TabProRate);
		
		dbJgccmx.insertJgccmx(scode,reportDate,shcode,indtCode,lxdm,ShareHDNum,Vposition,TabRate,TabProRate);	
		
    	
    	dbGudong.saveGudong(shcode, SHName, gdlx, lxdm, indtCode, instSName);

	}


	
}
