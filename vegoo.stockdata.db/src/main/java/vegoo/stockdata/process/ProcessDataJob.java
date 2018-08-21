package vegoo.stockdata.process;

import java.math.BigDecimal;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.Dictionary;
import java.util.List;
import java.util.Map;

import org.apache.karaf.scheduler.Job;
import org.apache.karaf.scheduler.JobContext;
import org.apache.karaf.scheduler.Scheduler;
import org.osgi.service.cm.ConfigurationException;
import org.osgi.service.cm.ManagedService;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.EmptyResultDataAccessException;

import vegoo.jdbcservice.JdbcService;
import vegoo.stockdata.core.BaseJob;


@Component (
		immediate = true, 
		configurationPid = "stockdata.processdata",
		property = {
		    Scheduler.PROPERTY_SCHEDULER_EXPRESSION + "= 0 0 */1 * * ?",   // 静态信息，每天7，8，18抓三次
		} 
	) 
public class ProcessDataJob extends BaseJob implements Job, ManagedService{
	private static final Logger logger = LoggerFactory.getLogger(ProcessDataJob.class);

    @Reference private JdbcService db;

	@Override
	public void updated(Dictionary<String, ?> properties) throws ConfigurationException {
		
		
	}

	@Override
	protected void executeJob(JobContext context) {
		
		processGdhs();
		processJgcc();
		processJgccmx();
		
	}

	private void processGdhs() {
		List<Date> reportDates = queryReportDateOfGdhsNullTransDate();
		for(Date rptDate:reportDates) {
			processGdhs(rptDate);
		}
	}
	
	private static final String QRY_NULL_STKS = "select distinct enddate from gdhs where EndTradeDate is null order by enddate desc";
	private List<Date> queryReportDateOfGdhsNullTransDate() {
		try {
			return db.queryForList(QRY_NULL_STKS, Date.class);
		}catch(Exception e) {
			logger.error("", e);
			return new ArrayList<>();
		}
	}

	private int processGdhs(Date rptDate) {
		List<Map<String, Object>> items = queryNullTransDateOfGDHS(rptDate);
		if(items==null||items.isEmpty()) {
			return 0;
		}
		for(Map<String, Object> item:items) {
		  updateNullTransDateOfGDHS(item);
		}
		return items.size();
	}
	
	private static final String QRY_GDHS_NULLTRNS="select stockcode, enddate from gdhs where enddate=? and EndTradeDate is null";
    private List<Map<String, Object>> queryNullTransDateOfGDHS(Date rptDate){
    	try {
    		return db.queryForList(QRY_GDHS_NULLTRNS, new Object[] {rptDate}, 
    				new int[] {Types.DATE});
    	}catch(Exception e) {
    		logger.error("",e);
    		return null;
    	}
    }
	
	private void updateNullTransDateOfGDHS(Map<String, Object> item) {
		String scode = (String) item.get("stockcode");
		Date enddate = (Date) item.get("enddate");

		Date endTradeDate = getLastTradeDate(enddate, scode);
		if(endTradeDate==null) {
			return;
		}
		
		updateNullTransDateOfGDHS(scode, enddate, endTradeDate);
	}

	/* 因为enddate有可能不是交易日，或者该股票当日停牌不交易，
	 * 求季报日期endDate对应的在最后一个交易日，以便在指标图上显示
	 * 
	 */
	private static final String QRY_LAST_TRNSDATE="select transDate from kdaydata where scode=? and transDate<=? order by transDate desc limit 1";
	private Date getLastTradeDate(Date endDate, String stockCode) {
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
	
	private static final String UPD_GDHS_TRNSDATE="update gdhs set EndTradeDate=? where stockcode=? and enddate=?";
	private void updateNullTransDateOfGDHS(String scode, Date enddate, Date endTradeDate) {
		try {
			db.update(UPD_GDHS_TRNSDATE, new Object[] {endTradeDate, scode, enddate},
					new int[] {Types.DATE, Types.VARCHAR, Types.DATE});
		}catch(EmptyResultDataAccessException e) {
			return;
		}catch(Exception e) {
			logger.error("",e);
			return;
		}
	}

	private void processJgcc() {
		// TODO Auto-generated method stub
		
	}
	
	private void processJgccmx() {
		List<Date> reportDates = queryJgccmxReportDates();
		
		for(int i=0;i<reportDates.size()-1;++i) { // 最早的日期不用算
			Date rdate = reportDates.get(i);
			processJgccmx(rdate);
		}
	}
	
	private static final String SQL_QRY_JGCCMX_RDATE="SELECT distinct rdate FROM jgccmx order by rdate desc";
	private List<Date> queryJgccmxReportDates() {
		try {
			return db.queryForList(SQL_QRY_JGCCMX_RDATE, Date.class);
		}catch(Exception e) {
			logger.error("", e);
			return new ArrayList<>();
		}
	}

	private void processJgccmx(Date rdate) {
		List<Map<String, Object>> items = queryJgccmx(rdate);
		Date prevRDate = getPreviousReportDate(rdate);
		
		for(Map<String, Object> item : items) {
			updateJgccmxData(item, prevRDate);
		}
	}

	private static final String SQL_QRY_JGCCMX="SELECT id,SCode,SHCode FROM jgccmx where rdate=? and PrevRDate is null";
	private List<Map<String, Object>> queryJgccmx(Date rdate){
		try {
			return db.queryForList(SQL_QRY_JGCCMX, new Object[] {rdate}, new int[] {Types.DATE});
		}catch(Exception e) {
			logger.error("", e);
			return new ArrayList<>();
		}
	}
	
	private void updateJgccmxData(Map<String, Object> item, Date prevRDate) {
		Integer id = (Integer) item.get("id");
		String SCode = (String) item.get("SCode");
		String SHCode = (String) item.get("SHCode");
		
		Map<String, Object> prevItem = queryPrevJgccmx(prevRDate, SHCode, SCode);
		if(prevItem==null || prevItem.isEmpty()) {
			return;
		}
		updateJgccmxPrevData(id, prevRDate, prevItem);
	}

	/***
	 * 将机构持股明细，补上上期持股数据，便于分析
	 * @param rdate
	 */
	private static final String SQL_QRY_JGCCMX_PREV="SELECT ShareHDNum,Vposition FROM jgccmx where rdate=? and shcode=? and scode=?";
	private Map<String, Object> queryPrevJgccmx(Date rdate,String SHCode, String SCode){
		try {
			return db.queryForMap(SQL_QRY_JGCCMX_PREV, new Object[] {rdate, SHCode, SCode}, 
					new int[] {Types.DATE, Types.VARCHAR, Types.VARCHAR});
		}catch(EmptyResultDataAccessException e) {
			return null;
		}catch(Exception e) {
			logger.error("", e);
			return null;
		}
	}
	
	private static final String SQL_UPD_JGCCMX_PREV="UPDATE jgccmx set PrevRDate=?, PrevHDNum=?,"
	        +" PrevVPosition=?, ChangeHDNum=ShareHDNum-? , ChangeValue=Vposition-? "
			+" where id=?";
	private void updateJgccmxPrevData(Integer id, Date prevRDate, Map<String, Object> prevItem) {
		try {
			double ShareHDNum = ((BigDecimal)prevItem.get("ShareHDNum")).doubleValue();
			double Vposition = ((BigDecimal)prevItem.get("Vposition")).doubleValue();
			
			db.update(SQL_UPD_JGCCMX_PREV, 
					new Object[] {prevRDate, ShareHDNum, Vposition, ShareHDNum, Vposition, id}, 
					new int[] {Types.DATE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.INTEGER});
		}catch(Exception e) {
			logger.error("", e);
		}
	}

	

}
