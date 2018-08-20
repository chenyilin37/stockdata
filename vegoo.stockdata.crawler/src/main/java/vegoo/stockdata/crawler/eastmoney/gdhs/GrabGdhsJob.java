package vegoo.stockdata.crawler.eastmoney.gdhs;


import java.beans.PropertyVetoException;
import java.io.File;
import java.io.IOException;
import java.sql.Types;
import java.text.ParseException;
import java.util.Date;
import java.util.Dictionary;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.sql.DataSource;

import org.apache.karaf.scheduler.Job;
import org.apache.karaf.scheduler.JobContext;
import org.apache.karaf.scheduler.Scheduler;
import org.osgi.service.cm.ConfigurationException;
import org.osgi.service.cm.ManagedService;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.osgi.service.event.EventAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.EmptyResultDataAccessException;

import com.google.common.base.Strings;

import vegoo.commons.JsonUtil;
import vegoo.jdbcservice.JdbcService;
import vegoo.stockdata.crawler.core.HttpClient;
import vegoo.stockdata.crawler.core.HttpRequestException;
import vegoo.stockdata.crawler.eastmoney.BaseGrabJob;
import vegoo.stockdata.crawler.eastmoney.ReportDataGrabJob;
import vegoo.stockdata.crawler.eastmoney.jgcc.GudongDataProcessor;
import vegoo.stockdata.crawler.eastmoney.jgcc.JgccListDto;


/*
 * 这些星号由左到右按顺序代表 ：     *    *    *    *    *    *   *     
                         格式： [秒] [分] [小时] [日] [月] [周] [年] 
 * 时间大小由小到大排列，从秒开始，顺序为 秒，分，时，天，月，年    *为任意 ？为无限制。 由此上面所配置的内容就是，在每天的16点26分启动buildSendHtml() 方法
	
	具体时间设定可参考
	"0/10 * * * * ?" 每10秒触发
	"0 0 12 * * ?" 每天中午12点触发 
	"0 15 10 ? * *" 每天上午10:15触发 
	"0 15 10 * * ?" 每天上午10:15触发 
	"0 15 10 * * ? *" 每天上午10:15触发 
	"0 15 10 * * ? 2005" 2005年的每天上午10:15触发 
	"0 * 14 * * ?" 在每天下午2点到下午2:59期间的每1分钟触发 
	"0 0/5 14 * * ?" 在每天下午2点到下午2:55期间的每5分钟触发 
	"0 0/5 14,18 * * ?" 在每天下午2点到2:55期间和下午6点到6:55期间的每5分钟触发 
	"0 0-5 14 * * ?" 在每天下午2点到下午2:05期间的每1分钟触发 
	"0 10,44 14 ? 3 WED" 每年三月的星期三的下午2:10和2:44触发 
	"0 15 10 ? * MON-FRI" 周一至周五的上午10:15触发 
	"0 15 10 15 * ?" 每月15日上午10:15触发 
	"0 15 10 L * ?" 每月最后一日的上午10:15触发 
	"0 15 10 ? * 6L" 每月的最后一个星期五上午10:15触发 
	"0 15 10 ? * 6L 2002-2005" 2002年至2005年的每月的最后一个星期五上午10:15触发 
	"0 15 10 ? * 6#3" 每月的第三个星期五上午10:15触发
	"0 0 06,18 * * ?"  在每天上午6点和下午6点触发 
	"0 30 5 * * ? *"   在每天上午5:30触发
	"0 0/3 * * * ?"    每3分钟触发
 */

@Component (
	immediate = true, 
	configurationPid = "stockdata.grab.gdhs",
	property = {
	    Scheduler.PROPERTY_SCHEDULER_EXPRESSION + "= 0 0 1,8,18 * * ?",   // 每小时更新一次
	    // Scheduler.PROPERTY_SCHEDULER_CONCURRENT + "= false"
	} 
) 
public class GrabGdhsJob extends ReportDataGrabJob implements Job, ManagedService{
	/*！！！ Job,ManagedService接口必须在此申明，不能一道父类中，否则，karaf无法辨认，Job无法执行  ！！！*/
	private static final Logger logger = LoggerFactory.getLogger(GrabGdhsJob.class);
	
	// 要抓起数据的季报，用逗号分割，对应报告日期；主要用于初始化
	private static final String PN_REPORT_DATES = "preload-reports";
	private static final String PN_URL_REPORT   = "url-report";
	private static final String PN_URL_LATEST   = "url-latest";
	private static final String PN_URL_T10LTGD   = "url-sdltgd";
	
	protected static final String TAG_REPORTDATE = "<REPORT_DATE>";
	protected static final String TAG_PAGENO     = "<PAGE_NO>";
	protected static final String TAG_STOCKCODE  = "<STOCK_CODE>"; 
	
	private String reportURL ;
	private String latestURL ;
	private String sdltgdURL ;
	
    @Reference
    private JdbcService db;
	
	@Override
	public void updated(Dictionary<String, ?> properties) throws ConfigurationException {
		this.latestURL = (String) properties.get(PN_URL_LATEST);
		this.reportURL = (String) properties.get(PN_URL_REPORT);
		this.sdltgdURL = (String) properties.get(PN_URL_T10LTGD);

		String reports   = (String) properties.get(PN_REPORT_DATES);
		
		logger.info("{} = {}", PN_URL_LATEST, latestURL);
		logger.info("{} = {}", PN_URL_REPORT, reportURL);
		logger.info("{} = {}", PN_URL_T10LTGD, sdltgdURL);
		logger.info("{} = {}", PN_REPORT_DATES, reports);
		
		if(!Strings.isNullOrEmpty(reports) && !Strings.isNullOrEmpty(reportURL)) {
			preloadGdhsData(reports.trim());
		}
	}
	
	private void preloadGdhsData(String data) {

		String[] reports = split(data, ",");
		for(String report : reports) {
			if(Strings.isNullOrEmpty(report)) {
				continue;
			}
			
			asyncExecute(new Runnable() {
				@Override
				public void run() {
					grabReportData(report.trim());
				}});
		}
	}
	
	private void grabReportData(String reportDate) {
		String urlPattern = reportURL.replaceAll(TAG_REPORTDATE, reportDate);
		
		grabGdhsData(reportDate, urlPattern);
	}
	
	@Override
	protected void executeJob(JobContext context) {
		grabLatestData();
	}
	
	private void grabLatestData() {
		if(Strings.isNullOrEmpty(latestURL)) {
			logger.error("没有在配置文件中设置{}参数！", PN_URL_LATEST);
			return;
		}
		
		String latestReportDate = getLatestReportDateAsString();
		
		grabGdhsData(latestReportDate, latestURL);
	}

	private void grabGdhsData(String reportDate, String urlPattern) {

		Set<String> cacheStocks = new HashSet<>();

		int page = 0;
		while(grabGdhsData(++page, urlPattern, cacheStocks, db) > page) ;
		
		grabSdltgdData(reportDate, cacheStocks, db);  // 十大流通股东
	}

	private int grabGdhsData(int page, String urlPattern, Set<String> cacheStocks, JdbcService db) {
		String url = urlPattern.replaceAll(TAG_PAGENO, String.valueOf(page));
		
		GdhsListDto listDto = requestData(url, GdhsListDto.class, "获取股东户数");

		if(listDto == null) {
			return 0;
		}
				
		saveData(listDto.getData(), cacheStocks, db);
		
		return listDto.getPages();
	}

	private void saveData(List<GdhsItemDto> items, Set<String> cacheStocks, JdbcService db) {
		for(GdhsItemDto item : items) {
			cacheStocks.add(item.getSecurityCode());
			
			if(existsGDHS(item.getSecurityCode(), item.getEndDate(), db)) {
				continue;
			}
			
			insertGdhsData(item.getSecurityCode(), item, db);
		}
	}
	
	private static final String SQL_EXIST_GDHS = "select stockcode from gdhs where stockcode=? and enddate=?";
	private boolean existsGDHS(String stockCode, Date endDate, JdbcService db) {
		try {
			String val = db.queryForObject(SQL_EXIST_GDHS, new Object[] {stockCode, endDate}, 
					new int[] {Types.VARCHAR, Types.DATE}, String.class);
		    // logger.info("vvvvv={}", val);
			return  val != null;
		}catch(EmptyResultDataAccessException e) {
			return false;
		}catch(Exception e) {
    		logger.error(String.format("stockCode:%s, endDate:%tF%n", stockCode, endDate), e);
			return false;
		}
	}

	private static final String SQL_ADDGDHS = "insert into gdhs(stockCode, EndDate, EndTradeDate, "
			+"HolderNum,PreviousHolderNum,HolderNumChange,HolderNumChangeRate,RangeChangeRate, "
			+"PreviousEndDate,HolderAvgCapitalisation,HolderAvgStockQuantity,TotalCapitalisation, "
			+"CapitalStock,NoticeDate) "         // ,CapitalStockChange,CapitalStockChangeEvent,ClosePrice
			+"values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)"; //,?,?,?
	private void insertGdhsData(String stockCode, GdhsItemDto item, JdbcService db) {
		Date endDate = item.getEndDate();
		if(endDate == null) {
			return;
		}
		// 如果股东户数为0，可能是数据错误，抛弃
		if(item.getHolderNum()<1) {
			return;
		}
		
		// 因为enddate有可能不是交易日，或者该股票当日停牌不交易，
		// 求季报日期endDate对应的在最后一个交易日，以便在指标图上显示
		Date endTradeDate = getLastTradeDate(endDate, stockCode, db);

		try {
		  db.update(SQL_ADDGDHS, new Object[] {stockCode, endDate, endTradeDate,
				item.getHolderNum(), item.getPreviousHolderNum(), item.getHolderNumChange(), 
				item.getHolderNumChangeRate(), item.getRangeChangeRate(), item.getPreviousEndDate(), 
				item.getHolderAvgCapitalisation(), item.getHolderAvgStockQuantity(), item.getTotalCapitalisation(), 
				item.getCapitalStock(), item.getNoticeDate()}, ///*, item.getCapitalStockChange(), item.getCapitalStockChangeEvent(), item.getClosePrice()*/
				new int[] {Types.VARCHAR, Types.DATE, Types.DATE, Types.DOUBLE,Types.DOUBLE,Types.DOUBLE,
						   Types.DOUBLE, Types.DOUBLE, Types.DATE,  Types.DOUBLE, Types.DOUBLE, Types.DOUBLE,
						   Types.DOUBLE,Types.DATE});  ///*,Types.DOUBLE,Types.VARCHAR,Types.DOUBLE*/
		}catch(Exception e) {
			logger.error("保存股东户数数据是出错: {}",JsonUtil.toJson(item), e);
		}
	}

	/* 因为enddate有可能不是交易日，或者该股票当日停牌不交易，
	 * 求季报日期endDate对应的在最后一个交易日，以便在指标图上显示
	 * 
	 */
	private static final String QRY_LAST_TRNSDATE="select transDate from kdaydata where scode=? and transDate<=?  order by transDate desc limit 1";
	private Date getLastTradeDate(Date endDate, String stockCode, JdbcService db) {
		try {
			return db.queryForObject(QRY_LAST_TRNSDATE, new Object[] {stockCode, endDate},
					new int[] {Types.VARCHAR, Types.DATE}, Date.class);
		}catch(EmptyResultDataAccessException e) {
			return null;
		}catch(Exception e) {
			logger.error("",e);
			return null;
		}
	}

	/*********************************************
	 * 十大流通股东
	 *********************************************/
	private void grabSdltgdData(String reportDate, Set<String> stocks, JdbcService db) {
		if(Strings.isNullOrEmpty(sdltgdURL)) {
			logger.error("没有在配置文件中设置{}参数！", PN_URL_T10LTGD);
			return;
		}

		String url = sdltgdURL.replaceAll(TAG_REPORTDATE, reportDate);

		for(String stkcode : stocks) {
			grabSdltgdData(url, stkcode, db);
		}
	}

	private void grabSdltgdData(String urlPattern, String stkcode, JdbcService db) {
		String url = urlPattern.replaceAll(TAG_STOCKCODE, stkcode );

		SdltgdDto[] listDtos = requestData(url, SdltgdDto[].class, "获取十大流通股东");
		if(listDtos == null || listDtos.length == 0) {
			return ;
		}
		
		for(SdltgdDto dto : listDtos) {
			processSdltgdData(dto, db);
		}
		
	}

	private void processSdltgdData(SdltgdDto dto, JdbcService db) {
		if(existSdltgdData(dto, db)) {
			return;
		}
		saveSdltgdData(dto, db);
		saveGudongData(dto, db);
	}

	private static String SQL_EXIST_SDLTGD = "select SCode from sdltgd where SCode=? and RDate=? and SHAREHDCODE=?";
	private boolean existSdltgdData(SdltgdDto dto, JdbcService db) {
		String scode = dto.getSCODE();
		Date rdate = dto.getRDATE();
		String shcode = dto.getSHAREHDCODE();
		
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
	private void saveSdltgdData(SdltgdDto dto, JdbcService db) {
		try {
			db.update(SQL_ADD_SDLTGD, new Object[] {dto.getCOMPANYCODE(), dto.getSHAREHDNAME(),
					dto.getSHAREHDTYPE(), dto.getSHARESTYPE(), dto.getRANK(), dto.getSCODE(), 
					dto.getRDATE(), dto.getSHAREHDNUM(), dto.getLTAG(), dto.getZB(),dto.getNDATE(),
					dto.getBZ(),dto.getBDBL(),dto.getSHAREHDCODE(),dto.getSHAREHDRATIO(),dto.getBDSUM()},
					new int[] {Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,Types.DOUBLE,
							Types.VARCHAR,Types.DATE,Types.DOUBLE,Types.DOUBLE,Types.DOUBLE,
							Types.DATE,Types.VARCHAR,Types.DOUBLE,Types.VARCHAR,Types.DOUBLE,Types.DOUBLE});
		}catch(Exception e) {
			logger.error("保存十大流通股东数据是出错: {}",JsonUtil.toJson(dto), e);
			
		}
	}
	
	private void saveGudongData(SdltgdDto dto, JdbcService db) {
		String SHCode = dto.getSHAREHDCODE();
		String SHName = dto.getSHAREHDNAME();
		String gdlx = dto.getSHAREHDTYPE();
		
		GudongDataProcessor gdProcessor = new GudongDataProcessor(db);
		gdProcessor.saveGudongData(SHCode, SHName, gdlx, null, null, null);
	}

	
}