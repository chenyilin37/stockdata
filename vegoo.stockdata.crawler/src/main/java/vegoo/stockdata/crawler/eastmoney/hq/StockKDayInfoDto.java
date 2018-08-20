package vegoo.stockdata.crawler.eastmoney.hq;

import java.util.Date;

public class StockKDayInfoDto {
	private double o;
	private double h;
	private double l;
	private double c;
	
	private double a;
	private double v;
	private double yc;
	
	private Date time;
	private String ticks;
	private int total;
	private String pricedigit;
	private String jys;
	private String Settlement;
	private int mk;
	
	private double sp;
	private boolean isrzrq;
	public double getO() {
		return o;
	}
	public void setO(double o) {
		this.o = o;
	}
	public double getH() {
		return h;
	}
	public void setH(double h) {
		this.h = h;
	}
	public double getL() {
		return l;
	}
	public void setL(double l) {
		this.l = l;
	}
	public double getC() {
		return c;
	}
	public void setC(double c) {
		this.c = c;
	}
	public double getA() {
		return a;
	}
	public void setA(double a) {
		this.a = a;
	}
	public double getV() {
		return v;
	}
	public void setV(double v) {
		this.v = v;
	}
	public double getYc() {
		return yc;
	}
	public void setYc(double yc) {
		this.yc = yc;
	}
	public Date getTime() {
		return time;
	}
	public void setTime(Date time) {
		this.time = time;
	}
	public String getTicks() {
		return ticks;
	}
	public void setTicks(String ticks) {
		this.ticks = ticks;
	}
	public int getTotal() {
		return total;
	}
	public void setTotal(int total) {
		this.total = total;
	}
	public String getPricedigit() {
		return pricedigit;
	}
	public void setPricedigit(String pricedigit) {
		this.pricedigit = pricedigit;
	}
	public String getJys() {
		return jys;
	}
	public void setJys(String jys) {
		this.jys = jys;
	}
	public String getSettlement() {
		return Settlement;
	}
	public void setSettlement(String settlement) {
		Settlement = settlement;
	}
	public int getMk() {
		return mk;
	}
	public void setMk(int mk) {
		this.mk = mk;
	}
	public double getSp() {
		return sp;
	}
	public void setSp(double sp) {
		this.sp = sp;
	}
	public boolean isIsrzrq() {
		return isrzrq;
	}
	public void setIsrzrq(boolean isrzrq) {
		this.isrzrq = isrzrq;
	}
	
	

}
