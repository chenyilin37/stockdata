package vegoo.stockdata.crawler.eastmoney.fhsg;

import java.util.ArrayList;
import java.util.List;

import vegoo.stockdata.crawler.eastmoney.gdhs.GdhsItemDto;

public class FhsgListDto {
	private int pages;
	private List<FhsgItemDto> data = new ArrayList<>();

	public int getPages() {
		return pages;
	}

	public void setPages(int pages) {
		this.pages = pages;
	}

	public List<FhsgItemDto> getData() {
		return data;
	}

	public void setData(List<FhsgItemDto> data) {
		this.data = data;
	}

}
