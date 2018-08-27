package vegoo.stockdata.db;

import java.util.Date;

public interface JgccmxPersistentService {

	void insertJgccmx(String scode, Date reportDate, String shcode, String indtCode, String lxdm, double shareHDNum,
			double vposition, double tabRate, double tabProRate);

	boolean existJgccmx(String scode, Date reportDate, String shcode);

	void settleJgccmx();

}
