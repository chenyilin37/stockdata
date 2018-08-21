package vegoo.stockdata.crawler.eastmoney;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.apache.commons.lang3.StringUtils;
import org.apache.karaf.scheduler.Job;
import org.apache.karaf.scheduler.JobContext;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

import vegoo.commons.JsonUtil;
import vegoo.commons.MyThreadPoolExecutor;
import vegoo.stockdata.core.BaseJob;
import vegoo.stockdata.crawler.core.HttpClient;
import vegoo.stockdata.crawler.core.HttpRequestException;

/*
 * 错误代码段分配你
 * 
 * 
 * 
 */

public abstract class BaseGrabJob extends BaseJob{
	private static final Logger logger = LoggerFactory.getLogger(BaseGrabJob.class);

	private static final HttpClient httpClient = HttpClient.newInstance();

	protected static final String TAG_REPORTDATE = "<REPORT_DATE>";
	protected static final String TAG_PAGENO     = "<PAGE_NO>";
	protected static final String TAG_STOCKCODE  = "<STOCK_CODE>";   // 600021
	protected static final String TAG_STOCKFCODE  = "<STOCK_FCODE>"; // 600021.sh
	protected static final String TAG_STOCKUCODE  = "<STOCK_UCODE>"; // 6000211
	
	//Redis KEY
	public static final String RK_CACHED_TABLES  = "CACHED_TABLES"; // 6000211
	public static final String RK_STOCKS  = "STOCKS"; 
	public static final String RK_BLOCKS  = "BLOCKS"; 	
	
	protected String requestData(String url, String requestCode) {
		String content = null;
		try {
			content = httpClient.syncRequest(url, 3);
		} catch (HttpRequestException e) {
			logger.error("{}//{}//HTTP错误: 代码：{},可能是网站API有变化，请根据下列url检查,URL如下: {}", requestCode, this.getClass().getSimpleName(),e.getResponseCode(), url);
		} catch (IOException e) {
			logger.error("{}//{}//HTTP错误: 可能是网络有问题或网站API有变化，请根据下列url检查,URL如下: {}", requestCode, this.getClass().getSimpleName(),url);
		}
		
		return content;
	}
	
	protected <T> T requestData(String url, Class<T> t, String requestCode) {
		String content = requestData(url, requestCode);

		if(Strings.isNullOrEmpty(content)) {
			logger.error("{}//{}//没有请求到网络数据，URL如下: {}", requestCode, this.getClass().getSimpleName(),url);
			return null;
		}
		
		T result = null;
		try {
			result = JsonUtil.fromJson(content, t);
		}catch(Exception e) {
			logger.error("{}//{}//JSON错误: 将json转化为对象是出错，请检查是否网站数据格式有变化，URL如下: {}, 返回的内容如下：{}", requestCode, this.getClass().getSimpleName(),url, content);
		}
		return result;
	}
	
	/*	String.split()会丢去后面为空的部分，用本函数可以避开该问题，
	 * 如：	
	 *  String data = "002067|景兴纸业|2017-06-30|券商|4|1|新进||11740||||";
	 *
	 * data.split("|"):  ["002067","景兴纸业","2017-06-30","券商","4","1","新进","","11740"]
	 * split(data) :     ["002067","景兴纸业","2017-06-30","券商","4","1","新进","","11740","","","",""]
	 */
	
	public static String getStockCodeWithMarketFix(String stkcode) {
		if(stkcode.startsWith("6")) {
			return stkcode+".sh";
		}else {
		    return stkcode+".sz";
		}
	}
	
	public static String getStockCodeWithMarketId(String stkcode) {
		if(stkcode.startsWith("6")) {
			return stkcode+"1";
		}else {
		    return stkcode+"2";
		}
	}

	
	
}
