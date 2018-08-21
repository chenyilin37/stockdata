package vegoo.stockdata.db;

public interface GudongPersistentService {

	void saveGudong(String hdCode, String hdName, String hdType);

	boolean existGudong(String sHCode);

	void inserteGudong(String SHCode, String SHName, String gdlx, String lxdm, String indtCode, String indtName);

	void saveGudong(String sHCode, String sHName, String gdlx, String lxdm, String indtCode, String instSName);

}
