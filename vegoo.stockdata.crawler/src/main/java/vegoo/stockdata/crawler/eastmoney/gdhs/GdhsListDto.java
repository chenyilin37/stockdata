package vegoo.stockdata.crawler.eastmoney.gdhs;

import java.util.*;



public class GdhsListDto {
	private int pages;
	private List<GdhsItemDto> data = new ArrayList<>();

	public int getPages() {
		return pages;
	}

	public void setPages(int pages) {
		this.pages = pages;
	}

	public List<GdhsItemDto> getData() {
		return data;
	}

	public void setData(List<GdhsItemDto> data) {
		this.data = data;
	}


}
