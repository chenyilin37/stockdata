package vegoo.stockdata.db;

import java.util.Set;

public interface BlockPersistentService extends PersistentService{

	boolean existBlock(String blockUCode);

	void insertBlock(String blkType, String marketid, String blkCode, String blkname, String blkUCode);

	void deleteAllStocksOfBlock(String blkUcode);

	void insertStockOfBlock(String blkUcode, String stkCode);

	void updateStocksOfBlock(String blkUcode, Set<String> stockCodes);

	void deleteStockOfBlock(String blkUcode, String stkCode);

}
