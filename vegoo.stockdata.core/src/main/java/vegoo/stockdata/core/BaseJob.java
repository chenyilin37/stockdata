package vegoo.stockdata.core;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.karaf.scheduler.JobContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import vegoo.commons.DateUtil;
import vegoo.commons.MyThreadPoolExecutor;

/*  log4j参数
#自定义样式   
#%c	输出所属的类目，通常就是所在类的全名 
#%C	输出Logger所在类的名称，通常就是所在类的全名 
#%d	输出日志时间点的日期或时间，默认格式为ISO8601，也可以在其后指定格式，比如：%d{yyy MMM dd HH:mm:ss , SSS}，%d{ABSOLUTE}，%d{DATE}
#%F	输出所在类的类名称，只有类名。
#%l	输出语句所在的行数，包括类名+方法名+文件名+行数
#%L	输出语句所在的行数，只输出数字
#%m	输出代码中指定的讯息，如log(message)中的message
#%M	输出方法名
#%p	输出日志级别，即DEBUG，INFO，WARN，ERROR，FATAL
#%r	输出自应用启动到输出该log信息耗费的毫秒数
#%t	输出产生该日志事件的线程名
#%n	输出一个回车换行符，Windows平台为“/r/n”，Unix平台为“/n”
#%%	用来输出百分号“%”

 */

public abstract class BaseJob {
	private static final Logger logger = LoggerFactory.getLogger(BaseJob.class);
	
	public BaseJob() {
		logger.info("Scheduler {} created.", this.getClass().getSimpleName());
	}
	
	private boolean running = false;
	public void execute(JobContext context) {
		logger.info("SchedulerJob {} triggered at {} running? : {}", context.getName(), new Date(), running);  
		
		if(running) {
			return;
		}
		
		running = true;
		try {
			executeJob(context);
		}finally {
			running = false;
		}
	}
	protected abstract void executeJob(JobContext context) ;
		
	
	
    // 核心线程数量
    private static int corePoolSize = 1;
    // 最大线程数量
    private static int maxPoolSize = 20;
    // 线程存活时间：当线程数量超过corePoolSize时，10秒钟空闲即关闭线程
    private static int keepAliveTime = 10*1000;
    // 缓冲队列
    // private static BlockingQueue<Runnable> workQueue = null;
    // 线程池
    private static ThreadPoolExecutor threadPoolExecutor = null;

    static {
        threadPoolExecutor = new MyThreadPoolExecutor(corePoolSize, maxPoolSize, keepAliveTime, TimeUnit.SECONDS);
    }
	
	static public Future<?> asyncExecute(Runnable runnable) {
		return threadPoolExecutor.submit(runnable);
	}
		
	public static String[] split(String data, String seperator) {
		return StringUtils.splitPreserveAllTokens(data, seperator);
	}
	
	public static String formatDate(Date date) {
		return formatDate(date, "yyyy-MM-dd");
	}
	
	public static String formatDate(Date date, String format) {
		return DateUtil.formatDateTime(date, format);
	}
	

			
}
