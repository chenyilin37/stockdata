package vegoo.stockdata.db.base;

public interface Redis {
	String KEY_CACHED_TBLS = "CACHED_TABLES";
	String KEY_BLOCKS = "blocks";
	
	String TABLENAME_BLOCK ="block";
	
	public static  String keyStocksOfBlock(String blkUcode) {
		return "blkstk_" + blkUcode;
	}

}
