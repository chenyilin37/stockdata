package vegoo.stockdata.export;

import java.io.File;
import java.sql.Types;
import java.util.Date;
import java.util.Dictionary;
import java.util.List;

import org.apache.karaf.scheduler.Job;
import org.apache.karaf.scheduler.JobContext;
import org.apache.karaf.scheduler.Scheduler;
import org.osgi.service.cm.ConfigurationException;
import org.osgi.service.cm.ManagedService;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

import vegoo.jdbcservice.JdbcService;
import vegoo.stockdata.core.utils.StockUtil;

@Component (
		immediate = true, 
		configurationPid = "stockdata.tdxdata",
		property = {
		    Scheduler.PROPERTY_SCHEDULER_EXPRESSION + "= 0 0 7,8,18 * * ?",   // 静态信息，每天7，8，18抓三次
		} 
	)
public class CreateTdxDataJob extends ExportDataJob implements Job, ManagedService{
	private static final Logger logger = LoggerFactory.getLogger(CreateTdxDataJob.class);

	private static final String PN_ROOTPATH = "datafile-rootpath";
	
    @Reference private JdbcService db;

	private String rootPath ="tdx-data";
	
    @Override
	public void updated(Dictionary<String, ?> properties) throws ConfigurationException {
		String path = (String) properties.get(PN_ROOTPATH);
		if(!Strings.isNullOrEmpty(path)) {
			this.rootPath = path;
		}
	}

	@Override
	protected void executeJob(JobContext context) {
		createGdhsData(db);  // 股东户数数据
		createJgccData(db);  // 机构持仓数据；
		//createBlockData(); // 自定义板块数据； 
	}

	private void createGdhsData(JdbcService db) {
		exportGdhsSerial(db);  //股东户数
		
		Date toDay = new Date();
		Date curReportDate = StockUtil.getReportDate(toDay,0);
		Date prevReportDate = StockUtil.getReportDate(toDay,-1);
		
		exportGdhsReport("股东-本季增减", prevReportDate, curReportDate, db);
		exportGdhsReport("股东-最新增减", curReportDate, toDay, db);
	}
	/*
	 * 在通达信中，深市和沪市分别是0和1
	 */
	private static final String SQL_HolderNum = "SELECT CONCAT_WS('|', left(d.stockcode,1)='6', d.stockcode, DATE_FORMAT(if(d.endtradedate is null, d.enddate, d.endtradedate),'%Y%m%d'), d.HolderNum) FROM gdhs d order by d.stockCode, d.enddate";
	private void exportGdhsSerial(JdbcService db) {
		
		try {
			List<String> items = db.queryForList(SQL_HolderNum, String.class);
			File file = getFile("股东-股东户数", rootPath);
			writeFile(items, file);
		} catch (Exception e) {
			logger.error("", e);
		}
	}

	/**
	 * d.HolderNumChangeRate<1000 剔除新股上市时股东变化
	 */
	private static final String SQL_ReportChangeRate = 
			"SELECT CONCAT_WS('|', left(d.stockcode,1)='6', d.stockcode, DATE_FORMAT(?,'%Y%m%d'), sum(d.HolderNumChangeRate))"
			+" FROM gdhs d where (d.enddate>? and d.enddate<=?) and d.HolderNumChangeRate<1000 group by d.stockcode order by d.stockcode" ;
	private void exportGdhsReport(String dataTitle, Date beginDate, Date endDate, JdbcService db) {
		try {
			List<String> items = db.queryForList(SQL_ReportChangeRate, 
					new Object[] {endDate, beginDate, endDate}, 
					new int[] {Types.DATE, Types.DATE, Types.DATE}, 
					String.class);
            File file = getFile(dataTitle, rootPath);
			writeFile(items, file);
		} catch (Exception e) {
			logger.error("", e);
		}
	}

	/**************
	 * 机构持股
	 * @param db
	 */
	private void createJgccData(JdbcService db) {
		exportJgccSerial(db);
		exportJgccReport(db);
	}
	
	private static final String SQL_JGCC_LTZB = "SELECT  CONCAT_WS('|', left(SCode,1)='6', SCode, DATE_FORMAT(EndTradeDate,'%Y%m%d'), sum(LTZB) ) FROM jgcc where lx in(1,2,3,5) and EndTradeDate is not null group by SCode, EndTradeDate order by SCode, EndTradeDate";
	private void exportJgccSerial(JdbcService db2) {
		try {
			List<String> items = db.queryForList(SQL_JGCC_LTZB, String.class);
			File file = getFile("主力-机构持仓", rootPath);
			writeFile(items, file);
		} catch (Exception e) {
			logger.error("", e);
		}
	}
	
	private void exportJgccReport(JdbcService db) {
		Date curReportDate = StockUtil.getReportDate();
		
		exportJgccZJReport(curReportDate, db);
		exportJgccCWReport(curReportDate, db);
	}
	
	private static final String SQL_JGCC_CHANGE ="SELECT  CONCAT_WS('|', left(SCode,1)='6', SCode, DATE_FORMAT(RDate,'%Y%m%d'), round(sum(ValueChange)/100000000 ,2)) FROM jgcc where lx in(1,2,3,5) and RDate=? group by SCode order by SCode";
	private void exportJgccZJReport(Date reportDate,JdbcService db) {
		try {
			List<String> items = db.queryForList(SQL_JGCC_CHANGE, new Object[] {reportDate}, new int[] {Types.DATE}, String.class);
			
			File file = getFile("主力-机构增仓", rootPath);

			writeFile(items, file);
		} catch (Exception e) {
			logger.error("", e);
		}
	}
	
	private static final String SQL_JGCC_CW ="SELECT  CONCAT_WS('|', left(SCode,1)='6', SCode, DATE_FORMAT(RDate,'%Y%m%d'), sum(LTZB)) FROM jgcc where lx in(1,2,3,5) and RDate=? group by SCode order by SCode";
	private void exportJgccCWReport(Date reportDate,JdbcService db) {
		try {
			List<String> items = db.queryForList(SQL_JGCC_CW, new Object[] {reportDate}, new int[] {Types.DATE}, String.class);
			
			File file = getFile("主力-机构仓位", rootPath);

			writeFile(items, file);
		} catch (Exception e) {
			logger.error("", e);
		}
	}
	
}
