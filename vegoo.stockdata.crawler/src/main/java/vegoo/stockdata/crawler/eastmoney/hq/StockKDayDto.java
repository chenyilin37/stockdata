package vegoo.stockdata.crawler.eastmoney.hq;

public class StockKDayDto {
	private String name;
	private String code;
	private StockKDayInfoDto info;
	private String[] data;
	private StockKDayFlowDto[] flow;  // rtntype=5（不复权）时，没有该数据项，rtntype=6（复权）有
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getCode() {
		return code;
	}
	public void setCode(String code) {
		this.code = code;
	}
	public StockKDayInfoDto getInfo() {
		return info;
	}
	public void setInfo(StockKDayInfoDto info) {
		this.info = info;
	}
	public String[] getData() {
		return data;
	}
	public void setData(String[] data) {
		this.data = data;
	}
	public StockKDayFlowDto[] getFlow() {
		return flow;
	}
	public void setFlow(StockKDayFlowDto[] flow) {
		this.flow = flow;
	}
	

}
