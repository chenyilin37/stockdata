package vegoo.stockdata.db;

import java.util.Date;

public interface JgccmxPersistentService {

	void insertJgccmx(String scode, Date reportDate, String shcode, String indtCode, String lxdm, double shareHDNum,
			double vposition, double tabRate, double tabProRate, int dataTag);

	boolean existJgccmx(String scode, Date reportDate, String shcode);
	boolean isNewJgccmx(String scode, Date reportDate, String shcode, int dataTag, boolean deleteOld);

	void settleJgccmx(boolean reset);


}
