package vegoo.stockdata.db;

import java.util.Date;

public interface SdltgdPersistentService {

	boolean existSdltgd(String scode, Date rdate, String sharehdcode);

	void insertSdltgd(String companycode, String sharehdname, String sharehdtype, String sharestype, double rank,
			String scode, Date rdate, double sharehdnum, double ltag, double zb, Date ndate, String bz, double bdbl,
			String sharehdcode, double sharehdratio, double bdsum);

	void setdbSdltgd();

}
