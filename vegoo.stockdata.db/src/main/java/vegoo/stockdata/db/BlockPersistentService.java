package vegoo.stockdata.db;

import java.util.Set;

import vegoo.stockdata.db.base.PersistentService;

public interface BlockPersistentService extends PersistentService{
	String TBL_BLOCK = "BLOCK";
	String TBL_STOCKOFBLOCK = "STOCKSOFBLOCK";
	
	boolean existBlock(String blockUCode);

	void insertBlock(String blkType, String marketid, String blkCode, String blkname, String blkUCode);

	void deleteAllStocksOfBlock(String blkUcode);

	void insertStockOfBlock(String blkUcode, String stkCode);

	void updateStocksOfBlock(String blkUcode, Set<String> stockCodes);

	void deleteStockOfBlock(String blkUcode, String stkCode);

	Set<String> getBlocksOfStock(String stockcode);

	Set<String> getStockCodesOfBlock(String blkUcode);

}
