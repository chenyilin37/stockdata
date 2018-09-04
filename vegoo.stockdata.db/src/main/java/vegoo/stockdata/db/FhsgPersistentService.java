package vegoo.stockdata.db;

import java.util.Date;

import vegoo.stockdata.core.model.FhsgItemDao;

public interface FhsgPersistentService {

	boolean existFhsg(String sCode, Date rDate);

	void insertFhsg(FhsgItemDao dao);

	double adjustWithHoldNum(Date reportDate, String scode, double hdNum);

	double calcLClose(String stockcode, Date transdate, double lClose);

}
