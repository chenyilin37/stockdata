package vegoo.stockdata.crawler.eastmoney.stock;

import java.beans.PropertyVetoException;
import java.sql.Types;
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

import com.google.common.base.Strings;

import vegoo.jdbcservice.JdbcService;
import vegoo.stockdata.crawler.eastmoney.BaseGrabJob;
import vegoo.stockdata.crawler.eastmoney.blk.BlockInfoDto;
import vegoo.stockdata.crawler.eastmoney.blk.GrabBlockJob;

@Component (
		immediate = true, 
		configurationPid = "stockdata.grab.stock",
		service = { Job.class,  ManagedService.class}, 
		property = {
		    Scheduler.PROPERTY_SCHEDULER_EXPRESSION + "= 0 0 1,18 * * ?", //  静态信息，每天7，8，18抓三次
		    // Scheduler.PROPERTY_SCHEDULER_CONCURRENT + "= false"
		} 
	)
public class GrabStockJob extends BaseGrabJob implements Job, ManagedService{
	/*！！！ Job,ManagedService接口必须在此申明，不能一道父类中，否则，karaf无法辨认，Job无法执行  ！！！*/
	private static final Logger logger = LoggerFactory.getLogger(GrabStockJob.class);

	static final String PN_URL_STOCK   = "url-stock";
	
	private String urlStock;
	
    @Reference
    private JdbcService db;
	
	@Override
	public void updated(Dictionary<String, ?> properties) throws ConfigurationException {
		this.urlStock = (String) properties.get(PN_URL_STOCK);
		
		logger.info("{} = {}", PN_URL_STOCK, urlStock);
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
		if(existStock(stkCode)) {
			return;
		}
		
		insertStockData(marketid, stkCode, stkName, stkUCode);
	}

    private static String SQL_EXIST_STK = "select 1 from stock where stockCode=?";
    private boolean existStock(String stkCode) {
    	try {
    		Integer val = db.queryForObject(SQL_EXIST_STK, new Object[] {stkCode}, new int[] {Types.VARCHAR}, Integer.class);
    		return  val !=null;
		}catch(EmptyResultDataAccessException e) {
			return false;
    	}catch(Exception e) {
    		logger.error("", e);
    		return false;
    	}
    }

	private static String SQL_NEW_STK = "insert into stock(marketid,stockCode,stockName,stockUcode,isNew) values (?,?,?,?,?)";
    private void insertStockData(String marketid, String stkCode, String stkName, String stkUCode) {
    	boolean isNew = stkName.startsWith("N");
    	try {
    	   db.update(SQL_NEW_STK, new Object[] {marketid, stkCode, stkName, stkUCode, (isNew?1:0)}, 
    			   new int[]{Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,Types.INTEGER});
    	}catch(Exception e) {
    		logger.error("插入stock记录出错：{}.{}.{}.{}/{}", marketid, stkCode, stkName, stkUCode, isNew);
    		logger.error("", e);
    	}
    }

}
