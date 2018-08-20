package vegoo.stockdata.db;

public interface RedisKey {
	public static  String Blocks() {
		return "blocks";
	}
	
	public static  String StocksOfBlock(String blkUcode) {
		return "blkstk_" + blkUcode;
	}

}
