package vegoo.stockdata.crawler.eastmoney.hq;

import java.util.Date;

public class StockKDayFlowDto {
	private Date time;
	private double  ltg;
	
	public Date getTime() {
		return time;
	}
	public void setTime(Date time) {
		this.time = time;
	}
	public double getLtg() {
		return ltg;
	}
	public void setLtg(double ltg) {
		this.ltg = ltg;
	}
	

}
