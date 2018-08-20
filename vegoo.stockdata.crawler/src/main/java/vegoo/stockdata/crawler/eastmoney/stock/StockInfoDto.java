package vegoo.stockdata.crawler.eastmoney.stock;

import java.util.*;

/*
#       {data:[
#              "1,601606,N军工,6016061",
#              "2,000068,华控赛格,0000682"
#               ... 
#              ],
#         recordsTotal:3550,
#         recordsFiltered:3550}
#  
*/
public class StockInfoDto {
    private List<String> data = new ArrayList<>();
    private int recordsTotal;
    private int recordsFiltered;
	public List<String> getData() {
		return data;
	}
	public void setData(List<String> data) {
		this.data = data;
	}
	public int getRecordsTotal() {
		return recordsTotal;
	}
	public void setRecordsTotal(int recordsTotal) {
		this.recordsTotal = recordsTotal;
	}
	public int getRecordsFiltered() {
		return recordsFiltered;
	}
	public void setRecordsFiltered(int recordsFiltered) {
		this.recordsFiltered = recordsFiltered;
	}
    
    
    
}
