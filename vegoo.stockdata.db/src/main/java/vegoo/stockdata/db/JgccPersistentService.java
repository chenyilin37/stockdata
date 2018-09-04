package vegoo.stockdata.db;

import java.util.Date;

public interface JgccPersistentService {

	boolean existJgcc(String scode, Date reportDate, int jglx);
	public void insertJgcc(String sCODE, Date rDATE,int jglx,double cOUNT,
			String cGChange, double shareHDNum,double vPosition,double tabRate,
			double lTZB,double shareHDNumChange,double rateChanges);
	void settleJgcc();
	
}
