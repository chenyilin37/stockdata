package vegoo.stockdata.db;

import java.util.Date;


public interface GdhsPersistentService {

	boolean existGDHS(String stockCode, Date endDate);

	void insertGdhs(String stockCode, Date endDate, double holderNum, double previousHolderNum,
			double holderNumChange, double holderNumChangeRate, double rangeChangeRate, Date previousEndDate,
			double holderAvgCapitalisation, double holderAvgStockQuantity, double totalCapitalisation,
			double capitalStock, Date noticeDate);

	void settleGdhs();


}
