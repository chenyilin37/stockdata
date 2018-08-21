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
    private static int maxPoolSize = 10;
    // 线程存活时间：当线程数量超过corePoolSize时，10秒钟空闲即关闭线程
    private static int keepAliveTime = 10*1000;
    // 缓冲队列
    // private static BlockingQueue<Runnable> workQueue = null;
    // 线程池
    private static ThreadPoolExecutor threadPoolExecutor = null;

    static {
        threadPoolExecutor = new MyThreadPoolExecutor(corePoolSize, maxPoolSize, keepAliveTime, TimeUnit.SECONDS);
    	//executorService = Executors.newCachedThreadPool();
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
		SimpleDateFormat dateFormat = new SimpleDateFormat(format);
		return dateFormat.format(date);
	}
	
	public static String getLatestReportDateAsString()  {
		return getLatestReportDateAsString(getLatestReportDate());
	}
	
	public static String getLatestReportDateAsString(Date theDate)  {
		 SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		 return dateFormat.format(getLatestReportDate(theDate));
	}

	public static Date getLatestReportDate()  {
		return getLatestReportDate(new Date());
	}
	
	public static Date getLatestReportDate(Date theDate)  {
		Calendar calendar = Calendar.getInstance();
		
		calendar.setTime(theDate);
		
		return getLatestReportDate(calendar);
	}
	
	private static Date getLatestReportDate(Calendar calendar) {
		calendar.add(Calendar.DATE, 1);  // 向后跳一天
		
		int month = calendar.get(Calendar.MONTH);  // 0 based
	
		int reportYear = calendar.get(Calendar.YEAR);
		int reportMonth = 0;
		int reportDay = 0;
		
		if(month<3) {   // 12-31
			-- reportYear;
			reportMonth = 11 ; // 去年12月
			reportDay = 31;  
		}else if(month<6) {    // 3-31
			reportMonth = 2;
			reportDay = 31;
		}else if(month<9) {    //6-30
			reportMonth = 5;
			reportDay = 30;
		}else {                // 9-30
			reportMonth = 8;
			reportDay = 30;
		}
		
		calendar.set(reportYear, reportMonth, reportDay);
		
		return calendar.getTime();
	}	
	
	public static Date getPreviousReportDate(Date theDate) {
		Calendar calendar = Calendar.getInstance();
		
		calendar.setTime(theDate);
		calendar.add(Calendar.MONTH, -3);
		
		return getLatestReportDate(calendar);
	}

	/*
	 * closed : 已经收市
	 */
	public static Date getLastTransDate(boolean closed) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(new Date());
		
		if(closed) {
			int hour = calendar.get(Calendar.HOUR_OF_DAY);
			if(hour>0 && hour<15) {
				calendar.add(Calendar.DATE, -1);
			}
		}
		
		return getCurrentTransDate(calendar);
	}
	
	private static Date getCurrentTransDate(Calendar calendar) {
		if(isHoliday(calendar)) {
			calendar.add(Calendar.DATE, -1);
			return getCurrentTransDate(calendar);
		}
		
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		calendar.set(Calendar.MILLISECOND, 0);
		
		return calendar.getTime();
	}
	
	private static boolean isHoliday(Calendar calendar) {
		int wd = calendar.get(Calendar.DAY_OF_WEEK);
		// SUNDAY, MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY, and SATURDAY
		if(wd==Calendar.SUNDAY || wd==Calendar.SATURDAY) {
			return true;
		}
		
		int m = calendar.get(Calendar.MONTH);
		int md = calendar.get(Calendar.DAY_OF_MONTH);
		if((m==1 && md==1) // 元旦
			||(m==5 && (md==1)) // 5.1
			||(m==10 && md==1)  // 10.1
			// 春节、端午、清明
			) {
			return true;
		}
		
		return false;
	}
	
	
	
/*
 *    public static void main(String[] args) {
	    String s = "2015-07-01,11.13,11.13,11.13,11.13,122,135741,-";
	    String[] flds = split(s,",");
		System.out.println(JsonUtil.toJson(flds));
		String zhenfu = "-".equals(flds[7].trim())?"0":flds[7].replaceAll("%", "");
		System.out.println(zhenfu);

	}
*/	
			
}
