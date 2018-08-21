package vegoo.stockdata.crawler.eastmoney.hq;

import java.beans.PropertyVetoException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.Dictionary;
import java.util.List;

import javax.sql.DataSource;

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
import org.springframework.jdbc.core.BatchPreparedStatementSetter;

import com.google.common.base.Strings;

import vegoo.commons.JsonUtil;
import vegoo.jdbcservice.JdbcService;
import vegoo.stockdata.core.model.KDayDao;
import vegoo.stockdata.core.model.StockCapital;
import vegoo.stockdata.crawler.eastmoney.BaseGrabJob;
import vegoo.stockdata.db.HQPersistentService;
import vegoo.stockdata.db.StockPersistentService;


@Component (
		immediate = true, 
		configurationPid = "stockdata.grab.hangqing",
		//service = { Job.class,  ManagedService.class}, 
		property = {
		    Scheduler.PROPERTY_SCHEDULER_EXPRESSION + "= 0 30 15,1 * * ?", //  静态信息，每天7，8，18抓三次
		    // Scheduler.PROPERTY_SCHEDULER_CONCURRENT + "= false"
		} 
	)
public class GrabHangqingJob extends BaseGrabJob implements Job,ManagedService {
	private static final Logger logger = LoggerFactory.getLogger(GrabHangqingJob.class);

	static final String PN_URL_LIVEDATA   = "url-livedata";
	static final String PN_URL_KDAY   = "url-kday";
	static final String PN_URL_MLINE   = "url-mline";
	
	private String urlLivedata;
	private String urlKday;
	
    @Reference
    private StockPersistentService dbStock;
    @Reference
    private HQPersistentService dbHQ;

	@Override
	public void updated(Dictionary<String, ?> properties) throws ConfigurationException {
		this.urlLivedata = (String) properties.get(PN_URL_LIVEDATA);
		this.urlKday = (String) properties.get(PN_URL_KDAY);

		logger.info("{} = {}", PN_URL_LIVEDATA, urlLivedata);
		logger.info("{} = {}", PN_URL_KDAY, urlKday);

		grabHistoryKDay();
	}

	@Override
	protected void executeJob(JobContext context) {
		if(Strings.isNullOrEmpty(urlLivedata)) {
			logger.error("没有在配置文件中设置{}参数！", PN_URL_LIVEDATA);
			return;
		}
		
		// liveUpdate();
	}
	
	private void grabHistoryKDay() {
		if(Strings.isNullOrEmpty(urlKday)) {
			logger.error("没有在配置文件中设置{}参数！", PN_URL_KDAY);
			return;
		}
		
		
		List<String> stockCodes = dbStock.queryAllStockCodes();
		if(stockCodes==null) {
			return;
		}
		grabHistoryKDay(stockCodes);
	}
 	

	private void grabHistoryKDay(List<String> stockCodes) {
		Date curTransDate = getLastTransDate(true);
		for(String stkcode : stockCodes) {
			Date lastDate = dbHQ.getLastTradeDate(stkcode);
			
			if(curTransDate.equals(lastDate)) {
				continue;
			}
			
			asyncExecute(new Runnable() {
				@Override
				public void run() {
					grabHistoryKDay(stkcode, lastDate);
				}});
		}
	}


	private void grabHistoryKDay(String stkcode, Date lastDate) {
		String stkUCode = getStockCodeWithMarketId(stkcode);
		
		String url = urlKday.replaceAll(TAG_STOCKUCODE, stkUCode);
		
		StockKDayDto theDto = requestData(url, StockKDayDto.class, "获取股票基本信息");
		
		if(theDto == null) {
			return ;
		}
		
		processHistoryKDay(url, stkcode, theDto, lastDate);
	}

	private void processHistoryKDay(String url,String stkcode, StockKDayDto theDto, Date lastDate) {
		// saveHistoryKDayInfo(stkcode, theDto.getInfo());
		saveHistoryKDayFlow(url, stkcode, theDto.getFlow());
		saveHistoryKDayData(url, stkcode, theDto, lastDate);
	}

	private void saveHistoryKDayFlow(String url, String stkcode, StockKDayFlowDto[] flows) {
		if(flows==null || flows.length ==0) {
			logger.error("日线数据格式有变化，没有流通股本数据， URL如下: {}", url);
			return;
		}
		
		for(StockKDayFlowDto dto: flows) {
			if(dbStock.existStockCapital(stkcode, dto.getTime())) {
				continue;
			}
			StockCapital dao = new StockCapital(stkcode, dto.getTime(), dto.getLtg());
			dbStock.insertStockCapital(dao);
		}
	}

	private void saveHistoryKDayData(String url,String stkcode, StockKDayDto kDto, Date lastDate) {
		List<KDayDao> newItems = getNewKDayData(url,stkcode, kDto, lastDate);
		
		if(newItems==null || newItems.isEmpty()) {
			return;
		}
		dbHQ.saveKDayData(newItems);
		

	}

	/*
 	// 日期、开盘、收盘、最高、最低、量、额，振幅、换手率
	"2008-05-12,1.62,1.79,1.85,1.62,182125,541669792,-",  
    "2008-05-13,1.80,2.02,2.02,1.79,49982,159771423,12.7%",
    "2008-05-14,2.09,2.07,2.15,1.94,49221,164265608,10.27%",
    */
	private List<KDayDao> getNewKDayData(String url,String stkcode, StockKDayDto kDto, Date lastDate) {
		List<KDayDao> result = new ArrayList<>();
		
		StockKDayFlowDto[] flow = kDto.getFlow();
		
		KDayDao prevDao = null;
		for(String item : kDto.getData()) {
			String[] flds = split(item, ",");
			if(flds.length != 8 && flds.length != 9) {
				logger.error("日线数据格式有变化，应该类似：2008-05-13,1.80,2.02,2.02,1.79,49982,159771423,12.7%，实际为：{} URL: {}", item, url);
			    return null;
			}
			
			try {
				Date tdate =  JsonUtil.parseDate(flds[0]);
				// String zhenfu = "-".equals(flds[7].trim())?"0":flds[7].replaceAll("%", "");

				KDayDao dao = new KDayDao();
				
				dao.setSCode(stkcode);
				dao.setTransDate(tdate);
				dao.setOpen(Double.parseDouble(flds[1]));
				dao.setClose(Double.parseDouble(flds[2]));
				dao.setHigh(Double.parseDouble(flds[3]));
				dao.setLow(Double.parseDouble(flds[4]));
				dao.setVolume(Double.parseDouble(flds[5]));
				dao.setAmount(Double.parseDouble(flds[6])/100);
				
				//计算量比
				double toRate = calcTurnoverRate(tdate, dao.getVolume(), flow);
				dao.setTurnoverRate(toRate);
				
				// 计算振幅和涨幅
				if(prevDao == null) {
					dao.setChangeRate(0);
					dao.setAmplitude(0);
				}else {  // TODO 除权日，这样算涨幅不对！！！
					double lclose = calcLClose(stkcode, tdate, prevDao.getClose());
					dao.setLClose(lclose);
					dao.setChangeRate((dao.getClose()-lclose)*100/lclose);
					dao.setAmplitude((dao.getHigh()-dao.getLow())*100/lclose);
				}

				if(lastDate==null || tdate.after(lastDate)) {
					result.add(dao);
				}
				
				prevDao = dao;
			}catch(Exception e) {
				logger.error("日线数据格式有变化，应该类似：2008-05-13,1.80,2.02,2.02,1.79,49982,159771423,12.7%，实际为：{} URL: {}", item, url);
				logger.error("",e);
			    return null;
			}			
		}
		return result;
	}
	
	private double calcLClose(String stkcode, Date tdate, double lClose) {
		// TODO 除权日，这样算涨幅不对！！！
		return lClose;
	}

	private double calcTurnoverRate(Date tdate, double volume, StockKDayFlowDto[] flow) {
		double ltg = 0;
		for(int i=0; i<flow.length; ++i) {
			StockKDayFlowDto dto = flow[flow.length-1-i];
			if(!tdate.before(dto.getTime())) {
				ltg = dto.getLtg();
				break;
			}
		}

		if(ltg==0) {
			return 0;
		}
		return volume*100/ltg;
	}

}
