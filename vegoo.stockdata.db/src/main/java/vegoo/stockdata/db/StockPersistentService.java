package vegoo.stockdata.db;

import java.util.Date;
import java.util.List;

import vegoo.stockdata.core.model.StockCapitalDao;

public interface StockPersistentService {

	List<String> getAllStockCodes();

	boolean existStockCapital(String stkcode, Date rDate);

	void insertStockCapital(StockCapitalDao dao);

	boolean existStock(String stkCode);

	void insertStock(String marketid, String stkCode, String stkName, String stkUCode);

}
