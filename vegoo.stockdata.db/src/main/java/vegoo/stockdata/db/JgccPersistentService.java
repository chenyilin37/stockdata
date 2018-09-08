package vegoo.stockdata.db;

import java.util.Date;

public interface JgccPersistentService {

	boolean isNewJgcc(String scode, Date reportDate, int lx, int dataTag, boolean deleteOld);
	boolean existJgcc(String scode, Date reportDate, int jglx);
	public void insertJgcc(String sCODE, Date rDATE,int jglx,double cOUNT,
			String cGChange, double shareHDNum,double vPosition,double tabRate,
			double lTZB,double shareHDNumChange,double rateChanges, int dataTag);
	void settleJgcc(boolean reset);
	
}
