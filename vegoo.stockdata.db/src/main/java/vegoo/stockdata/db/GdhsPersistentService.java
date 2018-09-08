package vegoo.stockdata.db;

import java.util.Date;
import java.util.Map;


public interface GdhsPersistentService {

	boolean existGDHS(String stockCode, Date endDate);

	void insertGdhs(String stockCode, Date endDate, double holderNum, double previousHolderNum,
			double holderNumChange, double holderNumChangeRate, double rangeChangeRate, Date previousEndDate,
			double holderAvgCapitalisation, double holderAvgStockQuantity, double totalCapitalisation,
			double capitalStock, Date noticeDate, int dataTag);

	void settleGdhs(boolean reset);

	boolean isNewGDHS(String stkCode, Date endDate, int dataTag, boolean deleteOld);

	Map<String, Object> queryGDHS(String stkCode, Date rDate);


}
