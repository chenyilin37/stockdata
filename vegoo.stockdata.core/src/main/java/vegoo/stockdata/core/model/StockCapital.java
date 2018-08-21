package vegoo.stockdata.core.model;

import java.util.Date;

public class StockCapital {
	private String stockCode;
	private Date transDate;
	private double ltg;
	
	
	public StockCapital(String stkcode, Date transDate, double ltg) {
		setStockCode(stkcode);
		setTransDate(transDate);
		setLtg(ltg);
	}
	
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
		return transDate;
	}

	public void setTransDate(Date transDate) {
		this.transDate = transDate;
	}
	
	

}
