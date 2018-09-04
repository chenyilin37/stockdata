package vegoo.stockdata.crawler.eastmoney.fhsg;

import java.util.Dictionary;
import java.util.List;
import java.util.concurrent.Future;

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

import vegoo.commons.JsonUtil;
import vegoo.stockdata.core.model.FhsgItemDao;
import vegoo.stockdata.core.utils.StockUtil;
import vegoo.stockdata.crawler.eastmoney.ReportDataGrabJob;
import vegoo.stockdata.db.FhsgPersistentService;
import vegoo.stockdata.db.StockPersistentService;

@Component (
	immediate = true, 
	service = { Job.class,  ManagedService.class},
	configurationPid = "stockdata.grab.fhsg",
	property = {
	    Scheduler.PROPERTY_SCHEDULER_EXPRESSION + "= 0 * 6-23 * * ?",   // 每小时更新一次
	    // Scheduler.PROPERTY_SCHEDULER_CONCURRENT + "= false"
	} 
) 
public class GrabFhsgJob extends ReportDataGrabJob implements Job, ManagedService {
	private static final Logger logger = LoggerFactory.getLogger(GrabFhsgJob.class);


	private static final String PN_URL_REPORT   = "url-report";
	private static final String PN_URL_STOCK   = "url-stock";
	
	//protected static final String TAG_REPORTDATE = "<REPORT_DATE>";
	//protected static final String TAG_PAGENO     = "<PAGE_NO>";
	//protected static final String TAG_STOCKCODE  = "<STOCK_CODE>"; 
	
    @Reference private StockPersistentService dbStock;
    @Reference private FhsgPersistentService dbFhsg;
	
	private String reportURL ;
	private String stockURL ;

    private Future<?> futureGrabbing;
	
	@Override
	public void updated(Dictionary<String, ?> properties) throws ConfigurationException {
		this.stockURL = (String) properties.get(PN_URL_STOCK);
		this.reportURL = (String) properties.get(PN_URL_REPORT);

		logger.info("{} = {}", PN_URL_REPORT, reportURL);
		logger.info("{} = {}", PN_URL_STOCK, stockURL);
		
		if(!Strings.isNullOrEmpty(stockURL)) {
			futureGrabbing = asyncExecute(new Runnable() {
				@Override
				public void run() {
					try {
					   grabFhsgByStocks();
					}finally {
					   futureGrabbing = null;
					}
				}
			});
		}
	}

	private void grabFhsgByStocks() {
		List<String> stockCodes = dbStock.getAllStockCodes();
		for(String stkCode : stockCodes) {
			grabFhsgByStock(stkCode);
		}
	}
	
	private void grabFhsgByStock(String stkCode) {
		String urlPattern = stockURL.replaceAll(TAG_STOCKCODE, stkCode);
		grabFhsgByPages(urlPattern);		
	}
	
	@Override
	protected void executeJob(JobContext context) {
		if(Strings.isNullOrEmpty(reportURL)) {
			logger.error("没有在配置文件中设置{}参数！", PN_URL_REPORT);
			return;
		}

		if(futureGrabbing != null) {
			if(futureGrabbing.isDone() || futureGrabbing.isCancelled()) {
			  futureGrabbing = null;
			}else {
			  return;
			}
		}
		
		grabFhsgByReport(StockUtil.getReportDateAsString());
	}
	
	private void grabFhsgByReport(String reportDate) {
		String url = reportURL.replaceAll(TAG_REPORTDATE, reportDate);
		grabFhsgByPages(url);
	}
	
	
	private void grabFhsgByPages(String urlPattern) {
		int page = 0;
		while(grabFhsgByPage(++page, urlPattern) > page) ;
	}

	private int grabFhsgByPage(int page, String urlPattern) {
		String url = urlPattern.replaceAll(TAG_PAGENO, String.valueOf(page));
		
		FhsgListDto listDto = requestData(url, FhsgListDto.class, "获取分红送股");

		if(listDto == null) {
			return 0;
		}
				
		saveFhsgData(listDto.getData());
		
		return listDto.getPages();
	}

	private void saveFhsgData(List<FhsgItemDto> items) {
		for(FhsgItemDto dto : items) {
			if(dbFhsg.existFhsg(dto.getCode(), dto.getReportingPeriod())) {
				continue;
			}
			
			FhsgItemDao dao = new FhsgItemDao();
			
			dao.setSCode(dto.getCode());
			dao.setRDate(dto.getReportingPeriod());
			dao.setSZZBL(dto.getSZZBL());
			dao.setSGBL(dto.getSGBL());
			dao.setZGBL(dto.getZGBL());
			dao.setXJFH(dto.getXJFH());
			dao.setGXL(dto.getGXL());
			dao.setYAGGR(dto.getYAGGR());
			dao.setGQDJR(dto.getGQDJR());
			dao.setCQCXR(dto.getCQCXR());
			dao.setYAGGRHSRZF(dto.getYAGGRHSRZF());
			dao.setGQDJRQSRZF(dto.getGQDJRQSRZF());
			dao.setCQCXRHSSRZF(dto.getCQCXRHSSRZF());
			dao.setYCQTS(dto.getYCQTS());
			dao.setTotalEquity(dto.getTotalEquity());
			dao.setEarningsPerShare(dto.getEarningsPerShare());
			dao.setNetAssetsPerShare(dto.getNetAssetsPerShare());
			dao.setMGGJJ(dto.getMGGJJ());
			dao.setMGWFPLY(dto.getMGWFPLY());
			dao.setJLYTBZZ(dto.getJLYTBZZ());
			dao.setResultsbyDate(dto.getResultsbyDate());
			dao.setProjectProgress(dto.getProjectProgress());
			dao.setAllocationPlan(dto.getAllocationPlan());			
			
			try {
				dbFhsg.insertFhsg(dao);
			}catch(Exception e) {
				logger.error("保存股东户数数据是出错: {}",JsonUtil.toJson(dto), e);
			}
		}

		
	}

}
