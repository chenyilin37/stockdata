package vegoo.stockdata.crawler.tryit;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.lang3.StringUtils;

import vegoo.commons.DateUtil;
import vegoo.commons.JsonUtil;
import vegoo.stockdata.crawler.eastmoney.gdhs.GdhsItemDto;
import vegoo.stockdata.crawler.eastmoney.jgcc.SdltgdDto;

public class TryIt {

	public static void main(String[] args) {
		long timestamp = 1372089600;
		Date date =  new Date(timestamp*1000);
		System.out.println(DateUtil.formatDateTime(date));	
		System.out.println(date.getTime());	

	}
	
	protected static <T extends Number> void setValue( T value) {
		System.out.println( String.valueOf(value));
	}
	
	
	private static void trySdltgfDto() {
		String data="{\"SecurityCode\":\"002370\",\"SecurityName\":\"亚太药业\",\"HolderNum\":13861.0,\"PreviousHolderNum\":16035.0,\"HolderNumChange\":-2174.0,\"HolderNumChangeRate\":-13.5578,\"RangeChangeRate\":5.09,\"EndDate\":\"2013-03-31T00:00:00\",\"PreviousEndDate\":\"2012-12-31T00:00:00\",\"HolderAvgCapitalisation\":115385.614313542,\"HolderAvgStockQuantity\":14717.55,\"TotalCapitalisation\":1.59936E9,\"CapitalStock\":2.04E8,\"NoticeDate\":\"2013-04-26T00:00:00\"}";
	
		GdhsItemDto dto=JsonUtil.fromJson(data, GdhsItemDto.class);
		System.out.println(JsonUtil.toJson(dto));
	}
	
	private static void tryStringList() {
		List<String> list = new ArrayList<>();
		list.add("psod,fsfndfs");
		list.add("psod,fsfndfs");
		list.add("psodf,sfndfs");
		
		System.out.println(JsonUtil.toJson(list));
	}
	
	private static void split() {
		String data = "002067|景兴纸业|2017-06-30|券商|4|1|新进||11740||||";
		String[] vals = data.split("\\|");
		SortedSet<String> keys = new TreeSet<>();
		for(String s:vals) {
			keys.add(s);
		}
		
	}
	private static void split(String data, String sep) {

		String[] val2 = StringUtils.splitPreserveAllTokens(data, sep);
		
		System.out.println(JsonUtil.toJson(val2));		

	}
	
	

}
