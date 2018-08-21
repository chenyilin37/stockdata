package vegoo.stockdata.db;

import java.util.Date;

public interface JgccPersistentService {

	boolean existJgcc(String scode, Date reportDate, String jglx);
	public void insertJgcc(String sCODE, Date rDATE,String jglx,double cOUNT,
			String cGChange, double shareHDNum,double vPosition,double tabRate,
			double lTZB,double shareHDNumChange,double rateChanges);
	
}
