package vegoo.stockdata.crawler.eastmoney.blk;


import java.beans.PropertyVetoException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.EmptyResultDataAccessException;

import com.google.common.base.Strings;

import vegoo.commons.JsonUtil;
import vegoo.jdbcservice.JdbcService;

import vegoo.redis.RedisService;
import vegoo.stockdata.crawler.core.HttpRequestException;
import vegoo.stockdata.crawler.eastmoney.BaseGrabJob;
import vegoo.stockdata.crawler.eastmoney.gdhs.GdhsListDto;
import vegoo.stockdata.db.BlockPersistentService;
import vegoo.stockdata.db.PersistentService;


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
	configurationPid = "stockdata.grab.block",
	//service = { Job.class,  ManagedService.class}, 
	property = {
	    Scheduler.PROPERTY_SCHEDULER_EXPRESSION + "= 0 0 1,18 * * ?", //  静态信息，每天7，8，18抓三次
	    // Scheduler.PROPERTY_SCHEDULER_CONCURRENT + "= false"
	} 
)
public class GrabBlockJob extends BaseGrabJob implements Job, ManagedService {
	/*！！！ Job,ManagedService接口必须在此申明，不能一道父类中，否则，karaf无法辨认，Job无法执行  ！！！*/
	private static final Logger logger = LoggerFactory.getLogger(GrabBlockJob.class);
	
	static final String PN_URL_BLKINFO   = "url-blockinfo";
	static final String PN_URL_STKBLK   = "url-stocks-of-block";
    static final String PN_BLOCK_TYPES   = "block-types";
	
	private static final String TAG_BLOCK_TYPE   = "<BLOCK_TYPE>";
	private static final String TAG_BLOCK_CODE = "<BLOCK_CODE>";
	// private static final String TAG_PAGENO     = "<PAGE_NO>";

	private String urlBlockinfo;
	private String urlStocksOfBlk;
	private String[] blockTypes = split("BKHY,BKDY,BKGN", ",");

    @Reference
    private BlockPersistentService dbBlock;
	
    
	public GrabBlockJob() {
		
	}

	@Override
	public void updated(Dictionary<String, ?> properties) throws ConfigurationException {
		this.urlBlockinfo = (String) properties.get(PN_URL_BLKINFO);
		this.urlStocksOfBlk = (String) properties.get(PN_URL_STKBLK);
		
		String s = (String) properties.get(PN_BLOCK_TYPES);
		if(!Strings.isNullOrEmpty(s)) {
			this.blockTypes = split(s.trim(), ",");
		}

		logger.info("{} = {}", PN_URL_BLKINFO, urlBlockinfo);
		logger.info("{} = {}", PN_URL_STKBLK, urlStocksOfBlk);
		logger.info("{} = {}", PN_BLOCK_TYPES, s);
	}

	@Override
	protected void executeJob(JobContext context) {
		
		if(Strings.isNullOrEmpty(urlBlockinfo)) {
			logger.error("没有在配置文件中设置{}参数！", PN_URL_BLKINFO);
			return;
		}
		
		if(Strings.isNullOrEmpty(urlStocksOfBlk)) {
			logger.error("没有在配置文件中设置{}参数！", PN_URL_STKBLK);
			return;
		}
		
		if(blockTypes == null) {
			logger.error("没有在配置文件中设置{}参数！", PN_BLOCK_TYPES);
			return;
		}

		for(String blkType : blockTypes) {
		   grabBlkInfo(blkType);		
		}
	}


	private void grabBlkInfo(String blkType) {
		String url = urlBlockinfo.replaceAll(TAG_BLOCK_TYPE, blkType);

		BlockInfoDto listDto = requestData(url, BlockInfoDto.class, "获取板块基本信息");
		
		if(listDto==null) {
			return ;
		}
		
		List<String> blkCodes = processBlockData(blkType,listDto.getData());
		
		grabStockOfBlock(blkCodes);
	}

	
    private List<String> processBlockData(String blkType, List<String> blocks) {
    	List<String> blkCodes = new ArrayList<>();
		for(String blkInfo : blocks) {
			
			String[] fields = split(blkInfo, ","); 
			
			if(fields.length != 4) {
				// 正确的格式: 1,BK0730,农药兽药,BK07301
				logger.error("板块的股票列表数据格式错误，应该类似“1,BK0730,农药兽药,BK07301”，接收到的是: {}", blkInfo);
				break;
			}
			 
			String marketid = fields[0];
			String blkCode  = fields[1];
			String blkname  = fields[2];
			String blkUCode = fields[3];
			
			saveBlockData(blkType, marketid, blkCode, blkname, blkUCode);
			
			blkCodes.add(blkUCode);
		}
		return blkCodes;
	}
    
    
    private void saveBlockData(String blkType, String marketid, String blkCode, String blkname, String blkUCode) {
		if(!dbBlock.existBlock(blkUCode)) {
			dbBlock.insertBlock(blkType, marketid, blkCode, blkname, blkUCode);
		}
	}


	private void grabStockOfBlock(List<String> blkCodes) {
		for(String blkCode : blkCodes) {
			String url = urlStocksOfBlk.replaceAll(TAG_BLOCK_CODE, blkCode);
			
			grabStockOfBlock(blkCode, url);
		}
	}

    
	private void grabStockOfBlock(String blkUcode, String urlPattern) {
		
		
		List<String> stksOfBlk = new ArrayList<>();
		
		int page = 0;
		while(grabStockOfBlock(blkUcode, ++page, urlPattern, stksOfBlk) > page) ;
		
		//db.deleteAllStocksOfBlock(blkUcode);
		
		saveStocksOfBlock(blkUcode, stksOfBlk);
	}
    
	private int grabStockOfBlock(String blkUcode, int page, String urlPattern, List<String> stksOfBlk) {
		String url = urlPattern.replaceAll(TAG_PAGENO, String.valueOf(page));
		
		StocksOfBlockDto listDto = requestData(url, StocksOfBlockDto.class, "获取板块的股票列表");

		if(listDto==null) {
			return 0;
		}
		
		stksOfBlk.addAll(listDto.getRank());
		
		return listDto.getPages();
	}

	private void saveStocksOfBlock(String blkUcode, List<String> stockInfos) {
		Set<String> stockCodes = new HashSet<>();
		for(String stkInfo: stockInfos) {
			//logger.info("{} stk-blk: {}", ++stkofblkcounter, stkInfo);
			
			String[] fields = split(stkInfo,","); 
			if(fields.length != 4) {
				// 正确的格式: 2,300589,江龙船艇,3005892
				logger.error("板块的股票列表数据格式错误，应该类似“2,300589,江龙船艇,3005892”，接收到的是: {}", stkInfo);
				break;
			}
			
			String marketid = fields[0];
			String stkCode  = fields[1];
			
			stockCodes.add(stkCode);
		}
		dbBlock.updateStocksOfBlock(blkUcode, stockCodes) ;
	}
	


}