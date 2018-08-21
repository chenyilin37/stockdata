package vegoo.stockdata.db;

import java.util.Date;
import java.util.List;

import vegoo.stockdata.core.model.StockCapital;

public interface StockPersistentService {

	List<String> queryAllStockCodes();

	boolean existStockCapital(String stkcode, Date rDate);

	void insertStockCapital(StockCapital dao);

	boolean existStock(String stkCode);

	void insertStock(String marketid, String stkCode, String stkName, String stkUCode);

}
