package vegoo.stockdata.crawler.eastmoney.stock;

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

import vegoo.stockdata.crawler.eastmoney.BaseGrabJob;
import vegoo.stockdata.db.StockPersistentService;

@Component (
		immediate = true, 
		configurationPid = "stockdata.grab.stock",
		service = { Job.class,  ManagedService.class}, 
		property = {
		    Scheduler.PROPERTY_SCHEDULER_EXPRESSION + "= 0 * 6-23/3 * * ?", //  静态信息，每小时1次
		    // Scheduler.PROPERTY_SCHEDULER_CONCURRENT + "= false"
		} 
	)
public class GrabStockJob extends BaseGrabJob implements Job, ManagedService{
	/*！！！ Job,ManagedService接口必须在此申明，不能一道父类中，否则，karaf无法辨认，Job无法执行  ！！！*/
	private static final Logger logger = LoggerFactory.getLogger(GrabStockJob.class);

	static final String PN_URL_STOCK   = "url-stock";
	
	private String urlStock;
	
    @Reference
    private StockPersistentService dbStock;
	
	@Override
	public void updated(Dictionary<String, ?> properties) throws ConfigurationException {
		this.urlStock = (String) properties.get(PN_URL_STOCK);
		
		logger.info("{} = {}", PN_URL_STOCK, urlStock);
		
		// 优先抓去
		new Thread(new Runnable() {
			@Override
			public void run() {
				grabStockInfoData();
				
			}}).start();
		
	}

	@Override
	protected void executeJob(JobContext context) {
		grabStockInfoData();
	}

	private void grabStockInfoData() {
		if(Strings.isNullOrEmpty(urlStock)) {
			logger.error("没有在配置文件中设置{}参数！", PN_URL_STOCK);
			return;
		}
		
		grabStockInfoData(urlStock);
	}

	private void grabStockInfoData(String url) {
		StockInfoDto listDto = requestData(url, StockInfoDto.class, "获取股票基本信息");
		
		if(listDto==null) {
			return ;
		}
		processStockInfoData(listDto.getData());
	}

	private void processStockInfoData(List<String> data) {
		for(String stkInfo : data) {
			String[] fields = split(stkInfo, ",");
			
			if(fields.length != 4) {
				// 正确的格式: 1,601606,N军工,6016061
				logger.error("股票数据格式错误，应该类似“1,601606,N军工,6016061”，接收到的是: {}", stkInfo);
				break;
			}
			
			String marketid = fields[0];
			String stkCode  = fields[1];
			String stkName  = fields[2];
			String stkUCode = fields[3];
			
			saveStockData(marketid, stkCode, stkName, stkUCode);
		}
	}

	private void saveStockData(String marketid, String stkCode, String stkName, String stkUCode) {
		if(dbStock.existStock(stkCode)) {
			return;
		}
		
		dbStock.insertStock(marketid, stkCode, stkName, stkUCode);
	}


}
