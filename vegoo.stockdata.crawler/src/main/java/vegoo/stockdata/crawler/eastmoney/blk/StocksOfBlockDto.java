package vegoo.stockdata.crawler.eastmoney.blk;

import java.util.*;

public class StocksOfBlockDto {
    private int pages;
    private int total;
    private List<String> rank = new ArrayList<>();
    
	public int getPages() {
		return pages;
	}
	public void setPages(int pages) {
		this.pages = pages;
	}
	public int getTotal() {
		return total;
	}
	public void setTotal(int total) {
		this.total = total;
	}
	public List<String> getRank() {
		return rank;
	}
	public void setRank(List<String> rank) {
		this.rank = rank;
	}
    
    
}
