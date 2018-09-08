package vegoo.stockdata.db;

import java.util.Date;
import java.util.List;

import vegoo.stockdata.core.model.KDayDao;

public interface HQPersistentService {

	Date getLastTradeDate(String stockCode);
	Date getLastTradeDate(String stockCode, Date endDate);
	
	void saveKDayData(List<KDayDao> newItems);
	double getClosePrice(String sCode, Date transDate);

}
