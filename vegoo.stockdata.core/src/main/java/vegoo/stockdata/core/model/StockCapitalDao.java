package vegoo.stockdata.core.model;

import java.util.Date;

public class StockCapitalDao {
	private String stockCode;
	private Date rDate;
	private double ltg;
	

	public String getStockCode() {
		return stockCode;
	}
	public void setStockCode(String stockCode) {
		this.stockCode = stockCode;
	}

	public double getLtg() {
		return ltg;
	}
	public void setLtg(double ltg) {
		this.ltg = ltg;
	}

	public Date getTransDate() {
		return rDate;
	}

	public void setTransDate(Date transDate) {
		this.rDate = transDate;
	}
	
}
