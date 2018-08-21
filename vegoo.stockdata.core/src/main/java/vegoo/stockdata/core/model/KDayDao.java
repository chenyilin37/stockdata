package vegoo.stockdata.core.model;

import java.util.Date;

public class KDayDao {
	private String SCode;
	private Date transDate;
	private double open;
	private double close;
	private double high;
	private double low;
	private double volume;
	private double amount;
	private double LClose;
	private double changeRate;
	private double amplitude;
	private double turnoverRate;  // 不复权及参数rtntype=5时，有该数据项，复权rtntype=6时，没有
	
	public String getSCode() {
		return SCode;
	}
	public void setSCode(String sCode) {
		SCode = sCode;
	}
	public Date getTransDate() {
		return transDate;
	}
	public void setTransDate(Date transDate) {
		this.transDate = transDate;
	}
	public double getOpen() {
		return open;
	}
	public void setOpen(double open) {
		this.open = open;
	}
	public double getClose() {
		return close;
	}
	public void setClose(double close) {
		this.close = close;
	}
	public double getHigh() {
		return high;
	}
	public void setHigh(double high) {
		this.high = high;
	}
	public double getLow() {
		return low;
	}
	public void setLow(double low) {
		this.low = low;
	}
	public double getVolume() {
		return volume;
	}
	public void setVolume(double volume) {
		this.volume = volume;
	}
	public double getAmount() {
		return amount;
	}
	public void setAmount(double amount) {
		this.amount = amount;
	}
	public double getLClose() {
		return LClose;
	}
	public void setLClose(double lClose) {
		LClose = lClose;
	}
	public double getChangeRate() {
		return changeRate;
	}
	public void setChangeRate(double changeRate) {
		this.changeRate = changeRate;
	}
	public double getAmplitude() {
		return amplitude;
	}
	public void setAmplitude(double amplitude) {
		this.amplitude = amplitude;
	}
	public double getTurnoverRate() {
		return turnoverRate;
	}
	public void setTurnoverRate(double turnoverRate) {
		this.turnoverRate = turnoverRate;
	}
	
	
	
}
