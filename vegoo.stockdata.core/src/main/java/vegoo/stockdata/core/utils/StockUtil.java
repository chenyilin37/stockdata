package vegoo.stockdata.core.utils;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class StockUtil {
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
	
	
     public static void main(String[] args) {
	    
		System.out.println(String.format("%tF", getLastTransDate(false)));
		System.out.println(String.format("%tF", getLastTransDate(true)));

	}
		
	
}
