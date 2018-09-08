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
import java.util.concurrent.Future;

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
import vegoo.stockdata.core.utils.StockUtil;
import vegoo.stockdata.crawler.core.ReportDataGrabJob;
import vegoo.stockdata.db.GudongPersistentService;
import vegoo.stockdata.db.JgccPersistentService;
import vegoo.stockdata.db.JgccmxPersistentService;
import vegoo.stockdata.db.SdltgdPersistentService;
import vegoo.stockdata.db.StockPersistentService;

/**
 * 抓取机构持仓
 * @author cyl
 *
 */

@Component (
		immediate = true, 
		configurationPid = "stockdata.grab.jgcc",
		service = { Job.class,  ManagedService.class},
		property = {
		    Scheduler.PROPERTY_SCHEDULER_EXPRESSION + "= 0 * 6-23 * * ?",   // 静态信息，每天7，8，18抓三次
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
	
	// private static final String PN_REPORT_DATES = "preload-reports";
	private static final String PN_URL_JGCC   = "url-jgcc";
	private static final String PN_URL_JGCCMX   = "url-jgccmx";
	private static final String PN_URL_SDLTGD   = "url-sdltgd";

	private static final String PN_PRELOADS   = "preloads";
	
	//private static final String TAG_REPORTDATE = "<REPORT_DATE>";
	//private static final String TAG_PAGENO     = "<PAGE_NO>";
	//private static final String TAG_STOCKCODE  = "<STOCK_FCODE>"; //带市场后缀，如：000001.sz
	private static final String TAG_JGLX       = "<JGLX>";

    @Reference private JgccPersistentService dbJgcc;
    @Reference private JgccmxPersistentService dbJgccmx;
    @Reference private SdltgdPersistentService dbSdltgd;
    @Reference private GudongPersistentService dbGudong;
    @Reference private StockPersistentService dbStock;
	
	private String jgccURL ;
	private String jgccmxURL ;
	private String sdltgdURL ;
	
	private int preload_years = 1;
	
    private Future<?> futureGrabJgcc;
    private Future<?> futureGrabJgccmx;
    private Future<?> futureGrabTop10;
	
    // private int stateTag;
    
	@Override
	public void updated(Dictionary<String, ?> properties) throws ConfigurationException {
		this.jgccURL = (String) properties.get(PN_URL_JGCC);
		this.jgccmxURL = (String) properties.get(PN_URL_JGCCMX);
		this.sdltgdURL = (String) properties.get(PN_URL_SDLTGD);

		logger.info("{} = {}", PN_URL_JGCC, jgccURL);
		logger.info("{} = {}", PN_URL_JGCCMX, jgccmxURL);
		logger.info("{} = {}", PN_URL_SDLTGD, sdltgdURL);	
		
		String preloads = (String) properties.get(PN_PRELOADS);
		
		if(!Strings.isNullOrEmpty(preloads)) {
			preload_years = Integer.parseInt(preloads);
		}
		preloadReportData(preload_years);
	}

	private void preloadReportData(int years) {
		List<String> reports = new ArrayList<>();

		Date now = new Date();
		for(int i=0; i < 4*years ; ++i) {
			reports.add(StockUtil.getReportDateAsString(now, -1 * i)) ;
		}
		
		
		if(!Strings.isNullOrEmpty(jgccURL)) {
			futureGrabJgcc = asyncExecute(new Runnable() {

				@Override
				public void run() {
					try {
						for(String report : reports) {
							grabJgccData(report);
						}
					}finally {
						futureGrabJgcc = null;
					}
					
				}});
		}
		
		if(!Strings.isNullOrEmpty(sdltgdURL)) {
			futureGrabTop10 = asyncExecute(new Runnable() {

				@Override
				public void run() {
					try {
						for(String report : reports) {
							grabSdltgdData(report);
						}
					}finally {
						futureGrabTop10 = null;
					}
				}});
		}
		
		if(!Strings.isNullOrEmpty(jgccmxURL)) {
			futureGrabJgccmx = asyncExecute(new Runnable() {

				@Override
				public void run() {
					try {
						for(String report : reports) {
							grabJgccmxData(report);
						}
					}finally {
						futureGrabJgccmx = null;
					}
				}});
		}
	}

	@Override
	protected void executeJob(JobContext context) {
		String latestReportDate = StockUtil.getReportDateAsString();

		if(futureGrabJgcc == null ) {
			grabJgccData(latestReportDate);
		}else if(futureGrabJgcc.isDone() || futureGrabJgcc.isCancelled()) {
			futureGrabJgcc = null;
			grabJgccData(latestReportDate);
		}
		
		if(futureGrabTop10 == null ) {
			grabSdltgdData(latestReportDate);
		}else if(futureGrabTop10.isDone() || futureGrabTop10.isCancelled()) {
			futureGrabTop10 = null;
			grabSdltgdData(latestReportDate);
		}
		
		if(futureGrabJgccmx == null ) {
			grabJgccmxData(latestReportDate);
		}else if(futureGrabJgccmx.isDone() || futureGrabJgccmx.isCancelled()) {
			futureGrabJgccmx = null;
			grabJgccmxData(latestReportDate);
		}
	}


	private void grabJgccData(String reportDate) {
		if(Strings.isNullOrEmpty(jgccURL)) {
			logger.error("没有在配置文件中设置{}参数！", PN_URL_JGCC);
			return;
		}

		futureGrabJgcc = asyncExecute(new Runnable() {

			@Override
			public void run() {
				try {
					String url = jgccURL.replaceAll(TAG_REPORTDATE, reportDate);
					
					// 机构类型代码：1-基金 2-QFII 3-社保 4-券商 5-保险 6-信托
					for(int i=1; i<=6; ++i) {
						grabJgccDataByLx(i, url);
					}
				}finally {
					futureGrabJgcc = null;
				}
				
			}});
	}

	private void grabJgccDataByLx(int jglx, String urlPattern) {
		String url = urlPattern.replaceAll(TAG_JGLX, String.valueOf(jglx) );

		int tatolPages = 0;
		int page = 0;
		while(true) {
		   int pc = grabJgccDataByPage(++page, url); 
		   if(pc>tatolPages) {
			  tatolPages = pc;
		   }
		   if( page >= tatolPages) {
			   break;
		   }
		}
		
	}
	
	private int grabJgccDataByPage(int page, String urlPattern) {
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
		
		processJgccData(dataDto);
		
		return dataDto.getTotalPage();
	}

	private void processJgccData(JgccListDataDto dataDto) {
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
		    processJgccData(splitSymbol, fieldNames, item.trim());
		}
	}

	private void processJgccData(String splitSymbol, String[] fieldNames, String data) {
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
		String jgLX  = fieldValues.get(F_JGLX);
		
		if(scode==null || rdate==null || jgLX==null) {
			logger.error("机构持股数据项为空：{}={}, {}={}, {}={};", F_SCODE,scode, F_RDATE,rdate, F_JGLX, jgLX);
			return;
		}
		
		scode = scode.trim();
		if(scode.length() !=6 ) {
			logger.info("股票代码错误：{} // {}", scode,data);
			return;
		}

		Date reportDate = null;
		try {
			reportDate = JsonUtil.parseDate(rdate);
		} catch (ParseException e) {
			logger.error("invalid report date:{}", rdate, e);
			return;
		}
		
		int lx = Integer.valueOf(jgLX);
		
		int dataTag = data.hashCode();
		
		// 数据是逐步公布，需要同步更新
		if(!dbJgcc.isNewJgcc(scode, reportDate, lx, dataTag, true)) {
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
		
		dbJgcc.insertJgcc(scode,reportDate,lx,cOUNT,cGChange,shareHDNum,vPosition,tabRate,lTZB,shareHDNumChange,rateChanges, dataTag);
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
	private void grabJgccmxData(String reportDate) {
		if(Strings.isNullOrEmpty(jgccmxURL)) {
			logger.error("没有在配置文件中设置{}参数！", PN_URL_JGCCMX);
			return;
		}

		futureGrabJgccmx = asyncExecute(new Runnable() {
			@Override
			public void run() {
				try {
					List<String> stocks = dbStock.getAllStockCodes();
					
					// 抓去机构持仓明细
					grabJgccmxData(reportDate, stocks);
				}finally {
					futureGrabJgccmx = null;
				}
			}});
		
	}

	private void grabJgccmxData(String reportDate, List<String> stocks) {
		String url = jgccmxURL.replaceAll(TAG_REPORTDATE, reportDate);

		for(String stkcode : stocks) {
			String stkFcode = getStockCodeWithMarketFix(stkcode);
			grabJgccmxData(url, stkFcode);
		}
	}

	private void grabJgccmxData(String urlPattern, String stkFcode) {
		String url = urlPattern.replaceAll(TAG_STOCKFCODE, stkFcode );
        
		int tatolPages = 0;
		
		int page = 0;
		while(true) {
		   int pc = grabJgccmxDataByPage(++page, url); 
		   if(pc>tatolPages) {
			  tatolPages = pc;
		   }
		   if( page >= tatolPages) {
			   break;
		   }
		}
	}

	private int grabJgccmxDataByPage(int page, String urlPattern) {
		String url = urlPattern.replaceAll(TAG_PAGENO, String.valueOf(page));
		
		JgccListDto listDto = requestData(url, JgccListDto.class, "获取机构持仓明细");

		if(listDto == null) {
			logger.error("没有抓取到Jgccmx数据, URL:{}", url);
			return 0;
		}
			
		JgccListDataDto[] dataDtos = listDto.getData();
		if(dataDtos==null || dataDtos.length==0) {
			logger.error("没有抓取到Jgccmx数据, URL:{}", url);
			return 0;
		}
		
		JgccListDataDto dataDto = dataDtos[0];
		
		List<String> items = dataDto.getData();
		
		if(items==null || items.size()==0) {// API正常，但没有数据
			// logger.error("抓取到Jgccmx数据没有明细条目(data[]为空), URL:{}\n DATA: {}", url, JsonUtil.toJson(dataDto));
			return 0;
		}

		processJgccmxData(items, dataDto.getSplitSymbol(),dataDto.getFieldName());
		
		return dataDto.getTotalPage();
	}

	private void processJgccmxData(List<String> items, String splitSymbol, String fieldNameList) {
		// 通配符转义
		if("|".equals(splitSymbol)) {  
			splitSymbol = "\\"+splitSymbol;
		}
		
		String[] fieldNames = split(fieldNameList, ",") ;
		
		for(String item : items) {
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
		    return;
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
		
		int dataTag = data.hashCode();
		if(!dbJgccmx.isNewJgccmx(scode, reportDate, shcode, dataTag, true)) {
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

		dbJgccmx.insertJgccmx(scode,reportDate,shcode,indtCode,lxdm,ShareHDNum,Vposition,TabRate,TabProRate,dataTag);	

		dbGudong.saveGudong(shcode, SHName, gdlx, lxdm, indtCode, instSName);
	}

	/*********************************************
	 * 十大流通股东
	 *********************************************/
	protected void grabSdltgdData(String reportDate) {
		if(Strings.isNullOrEmpty(sdltgdURL)) {
			logger.error("没有在配置文件中设置{}参数！", PN_URL_SDLTGD);
			return;
		}
		
		futureGrabTop10 = asyncExecute(new Runnable() {
			@Override
			public void run() {
				try {
					String url = sdltgdURL.replaceAll(TAG_REPORTDATE, reportDate);

					List<String> stocks = dbStock.getAllStockCodes();
					
					for(String stkcode : stocks) {
						grabSdltgdData(url, stkcode);
					}
				}finally {
					futureGrabTop10 = null;
				}
			}});
	}
	
	private void grabSdltgdData(String urlPattern, String stkcode) {
		String url = urlPattern.replaceAll(TAG_STOCKCODE, stkcode );

		SdltgdDto[] listDtos = requestData(url, SdltgdDto[].class, "获取十大流通股东");
		
		if(listDtos == null ) {
			logger.error("没有抓取到十大股东数据, URL:{}", url);
			return;
		}
		if(listDtos.length == 0) { // API正常，但没有数据
			return;
		}
		
		for(SdltgdDto dto : listDtos) {
			try {
				processSdltgdData(dto);
			}catch(Exception e) {
				logger.error("保存十大流通股东数据是出错: {}",JsonUtil.toJson(dto), e);
			}			
		}
	}

	private void processSdltgdData(SdltgdDto dto) {
		int dataTag = dto.hashCode();
		

		if(!dbSdltgd.isNewSdltgd(dto.getSCODE(),dto.getRDATE(),dto.getSHAREHDCODE(),dataTag, true)) {
			return;
		}
		
		dbSdltgd.insertSdltgd(dto.getCOMPANYCODE(), dto.getSHAREHDNAME(),
				dto.getSHAREHDTYPE(), dto.getSHARESTYPE(), dto.getRANK(), dto.getSCODE(), 
				dto.getRDATE(), dto.getSHAREHDNUM(), dto.getLTAG(), dto.getZB(),dto.getNDATE(),
				dto.getBZ(),dto.getBDBL(),dto.getSHAREHDCODE(),dto.getSHAREHDRATIO(),dto.getBDSUM(),dataTag);
		
		dbGudong.saveGudong(dto.getSHAREHDCODE(),dto.getSHAREHDNAME(),dto.getSHAREHDTYPE());
	}
	
}
