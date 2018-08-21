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
		Date curReportDate = getLatestReportDate(toDay);
		Date prevReportDate = getPreviousReportDate(toDay);
		
		exportGdhsReport("股东-本季增减", prevReportDate, curReportDate, db);
		exportGdhsReport("股东-最新增减", curReportDate, toDay, db);
	}
	
	private static final String SQL_HolderNum = "SELECT CONCAT_WS('|', if(s.marketid=1,1,0), d.stockcode, DATE_FORMAT(if(d.endtradedate is null, d.enddate, d.endtradedate),'%Y%m%d'), d.HolderNum) FROM gdhs d, stock s where d.stockcode=s.stockcode  order by d.stockCode, d.enddate";
	private void exportGdhsSerial(JdbcService db) {
		List<String> items = db.queryForList(SQL_HolderNum, String.class);
		File file = getFile("股东-股东户数", rootPath);
		
		try {
			writeFile(items, file);
		} catch (Exception e) {
			logger.error("", e);
		}
	}

	private static final String SQL_ReportChangeRate = 
			"SELECT CONCAT_WS('|', if(s.marketid=1,1,0), d.stockcode, DATE_FORMAT(d.enddate,'%Y%m%d'), sum(d.HolderNumChangeRate))"
			+" FROM gdhs d, stock s where d.stockcode=s.stockcode and (d.enddate>? and d.enddate<=?) and d.HolderNumChangeRate<10000 group by d.stockcode order by d.stockcode" ;
	private void exportGdhsReport(String dataTitle, Date beginDate, Date endDate, JdbcService db) {
		try {
			List<String> items = db.queryForList(SQL_ReportChangeRate, 
					new Object[] {beginDate, endDate}, 
					new int[] {Types.DATE, Types.DATE}, 
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
		// exportJgccSerial(db);
		// exportJgccReport(db);
		
	}

	
}
