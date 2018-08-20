package vegoo.stockdata.crawler.eastmoney.jgcc;

import java.util.ArrayList;
import java.util.List;

/*
 * 数据结构
#       {
#       	"Message":""
#       	"Status":0
#       	"Data":[
#       		{
#       			"TableName":"RptMainDataMap"
#       			"TotalPage":511
#       			"ConsumeMSecond":1193
#       			"SplitSymbol":"|"
#       			"FieldName":"SCode,SName,RDate,LXDM,LX,Count,CGChange,ShareHDNum,VPosition,TabRate,LTZB,ShareHDNumChange,RateChange"
#       			"Data":[
#       				"600887|伊利股份|2018-06-30|基金|1|518|增持|784394737|21884613162.3|12.90442854|13.00094057|23896629|3.14223385286844"
#       				"000333|美的集团|2018-06-30|基金|1|507|增持|431672071|22541915547.62|6.51550737|6.55696217000001|21427708|5.22315720399064"
#       			    "600036|招商银行|2018-06-30|基金|1|454|减持|685813530|18132909733.2|2.71934072|3.32452081|-53778029|-7.27131459865674"
#       			]
#       		}
#       	]
#       }
#

 */
public class JgccListDataDto {
	private String TableName;
	private int TotalPage;
	private int ConsumeMSecond;
	private String SplitSymbol;
	private String FieldName;

	private List<String> Data = new ArrayList<>();

	public String getTableName() {
		return TableName;
	}

	public void setTableName(String tableName) {
		TableName = tableName;
	}

	public int getTotalPage() {
		return TotalPage;
	}

	public void setTotalPage(int totalPage) {
		TotalPage = totalPage;
	}

	public int getConsumeMSecond() {
		return ConsumeMSecond;
	}

	public void setConsumeMSecond(int consumeMSecond) {
		ConsumeMSecond = consumeMSecond;
	}

	public String getSplitSymbol() {
		return SplitSymbol;
	}

	public void setSplitSymbol(String splitSymbol) {
		SplitSymbol = splitSymbol;
	}

	public String getFieldName() {
		return FieldName;
	}

	public void setFieldName(String fieldName) {
		FieldName = fieldName;
	}

	public List<String> getData() {
		return Data;
	}

	public void setData(List<String> data) {
		Data = data;
	}

}
