package vegoo.stockdata.core.utils;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class StockUtil {
	public static String getReportDateAsString() {	
		return getReportDateAsString(new Date(),0);
	}

	public static Date getReportDate() {	
		return getReportDate(new Date(),0);
	}
	
	public static String getReportDateAsString(Date theDate, int previous)  {
		 SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		 return dateFormat.format(getReportDate(theDate, previous));
	}
	
	/*
	 * previous = 0 刚过去的那个季度，出数据报表的季度
	 */
	public static Date getReportDate(Date theDate, int previous) {
		Calendar calendar = Calendar.getInstance();
		
		calendar.setTime(theDate);
		calendar.add(Calendar.DATE, 1);  // 向后跳一天
		
		if(previous != 0) {
		  calendar.add(Calendar.MONTH, 3*previous);
		}
		
		return getReportDate(calendar);
	}

	private static Date getReportDate(Calendar calendar) {
		
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
	
	/*
	 * closed : 已经收市，排除法定节假日，
	 */
	public static Date getLastTransDate(boolean closed) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(new Date());
		
		if(closed) {
			int hour = calendar.get(Calendar.HOUR_OF_DAY);
			if(hour<15) {
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
	
	
     public static void main(String[] args) {
	    Date now = new Date();
		for(int i=0; i<4*6; ++i) {
			String rptDate = StockUtil.getReportDateAsString(now, -1 * i);
			System.out.println(rptDate);
		}
	}
		
	
}
