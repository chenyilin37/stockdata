package vegoo.stockdata.crawler.eastmoney.jgcc;

import java.util.Date;

/* 数据结构如下：
  [
    {"COMPANYCODE":"80174266","SSNAME":"","SHAREHDNAME":"吴涵渠","SHAREHDTYPE":"个人","SHARESTYPE":"A股","RANK":1.0,"SCODE":"002587","SNAME":"奥拓电子","RDATE":"2018-03-31T00:00:00","SHAREHDNUM":42789166.0,"LTAG":291822112.12,"ZB":0.108938651691787,"NDATE":"2018-04-27T00:00:00","BZ":"增加","BDBL":0.00996466754201791,"SHAREHDCODE":"4d0115402714866a39ceece679207051","SHAREHDRATIO":6.9987,"BDSUM":422173.0},
    {"COMPANYCODE":"80174266","SSNAME":"","SHAREHDNAME":"山东省国有资产投资控股有限公司","SHAREHDTYPE":"投资公司","SHARESTYPE":"A股","RANK":2.0,"SCODE":"002587","SNAME":"奥拓电子","RDATE":"2018-03-31T00:00:00","SHAREHDNUM":16743338.0,"LTAG":114189565.16,"ZB":0.0426275348890852,"NDATE":"2018-04-27T00:00:00","BZ":"增加","BDBL":0.145242990718131,"SHAREHDCODE":"80003035","SHAREHDRATIO":2.7386,"BDSUM":2123438.0},
    {"COMPANYCODE":"80174266","SSNAME":"","SHAREHDNAME":"郭卫华","SHAREHDTYPE":"个人","SHARESTYPE":"A股","RANK":3.0,"SCODE":"002587","SNAME":"奥拓电子","RDATE":"2018-03-31T00:00:00","SHAREHDNUM":9221218.0,"LTAG":62888706.76,"ZB":0.023476668273367,"NDATE":"2018-04-27T00:00:00","BZ":"增加","BDBL":1.14484140736974,"SHAREHDCODE":"a929d468695b3a6d07e21aec8b523bd1","SHAREHDRATIO":1.5082,"BDSUM":4921964.0},
    {"COMPANYCODE":"80174266","SSNAME":"","SHAREHDNAME":"黄斌","SHAREHDTYPE":"个人","SHARESTYPE":"A股","RANK":4.0,"SCODE":"002587","SNAME":"奥拓电子","RDATE":"2018-03-31T00:00:00","SHAREHDNUM":7226287.0,"LTAG":49283277.34,"ZB":0.0183976935310655,"NDATE":"2018-04-27T00:00:00","BZ":"不变","BDBL":0.0,"SHAREHDCODE":"5ea5236550ed4962752a79d8f340aebe","SHAREHDRATIO":1.182,"BDSUM":0.0},
    {"COMPANYCODE":"80174266","SSNAME":"","SHAREHDNAME":"赵旭峰","SHAREHDTYPE":"个人","SHARESTYPE":"A股","RANK":5.0,"SCODE":"002587","SNAME":"奥拓电子","RDATE":"2018-03-31T00:00:00","SHAREHDNUM":5164709.0,"LTAG":35223315.38,"ZB":0.0131490395218368,"NDATE":"2018-04-27T00:00:00","BZ":"减少","BDBL":-3.87243352048159E-07,"SHAREHDCODE":"42a9bde27a97dbc02bd7ef125f29244b","SHAREHDRATIO":0.8448,"BDSUM":-2.0},
    {"COMPANYCODE":"80174266","SSNAME":"","SHAREHDNAME":"沈永健","SHAREHDTYPE":"个人","SHARESTYPE":"A股","RANK":6.0,"SCODE":"002587","SNAME":"奥拓电子","RDATE":"2018-03-31T00:00:00","SHAREHDNUM":4849999.0,"LTAG":33076993.18,"ZB":0.0123478067267428,"NDATE":"2018-04-27T00:00:00","BZ":"新进","BDBL":"-","SHAREHDCODE":"945a3dc9bc1547bd74bf0b63505acb36","SHAREHDRATIO":0.7933,"BDSUM":"-"},
    {"COMPANYCODE":"80174266","SSNAME":"","SHAREHDNAME":"陈国雄","SHAREHDTYPE":"个人","SHARESTYPE":"A股","RANK":7.0,"SCODE":"002587","SNAME":"奥拓电子","RDATE":"2018-03-31T00:00:00","SHAREHDNUM":4794059.0,"LTAG":32695482.38,"ZB":0.0122053868399977,"NDATE":"2018-04-27T00:00:00","BZ":"不变","BDBL":0.0,"SHAREHDCODE":"56ddce774dc149c4377ebb738f099aed","SHAREHDRATIO":0.7841,"BDSUM":0.0},
    {"COMPANYCODE":"80174266","SSNAME":"","SHAREHDNAME":"邱荣邦","SHAREHDTYPE":"个人","SHARESTYPE":"A股","RANK":8.0,"SCODE":"002587","SNAME":"奥拓电子","RDATE":"2018-03-31T00:00:00","SHAREHDNUM":4542958.0,"LTAG":30982973.56,"ZB":0.0115660987459399,"NDATE":"2018-04-27T00:00:00","BZ":"增加","BDBL":2.20120947655899E-07,"SHAREHDCODE":"904b1454e7f4c67145d3a3a6949480f9","SHAREHDRATIO":0.7431,"BDSUM":1.0},
    {"COMPANYCODE":"80174266","SSNAME":"","SHAREHDNAME":"中国建设银行股份有限公司-景顺长城量化精选股票型证券投资基金","SHAREHDTYPE":"基金","SHARESTYPE":"A股","RANK":9.0,"SCODE":"002587","SNAME":"奥拓电子","RDATE":"2018-03-31T00:00:00","SHAREHDNUM":4529795.0,"LTAG":30893201.9,"ZB":0.0115325865369799,"NDATE":"2018-04-27T00:00:00","BZ":"减少","BDBL":-0.466399304520733,"SHAREHDCODE":"000978","SHAREHDRATIO":0.7409,"BDSUM":-3959315.0},
    {"COMPANYCODE":"80174266","SSNAME":"","SHAREHDNAME":"周维君","SHAREHDTYPE":"个人","SHARESTYPE":"A股","RANK":10.0,"SCODE":"002587","SNAME":"奥拓电子","RDATE":"2018-03-31T00:00:00","SHAREHDNUM":4315039.0,"LTAG":29428565.98,"ZB":0.0109858306342656,"NDATE":"2018-04-27T00:00:00","BZ":"新进","BDBL":"-","SHAREHDCODE":"62f7dd5629eab5a2a6a61810e9d0c4b9","SHAREHDRATIO":0.7058,"BDSUM":"-"}
  ]
*/
public class SdltgdDto {
	private String COMPANYCODE;          
	private String SSNAME;               
	private String SHAREHDNAME;          
	private String SHAREHDTYPE;          
	private String SHARESTYPE;           
	private double RANK;                 
	private String SCODE;                
	private String SNAME;                
	private Date RDATE;                
	private double SHAREHDNUM;           
	private double LTAG;                 
	private double ZB;                   
	private Date NDATE;                
	private String BZ;                   
	private double BDBL;                 
	private String SHAREHDCODE;          
	private double SHAREHDRATIO;         
	private double BDSUM;
	
	
	public String getCOMPANYCODE() {
		return COMPANYCODE;
	}
	public void setCOMPANYCODE(String cOMPANYCODE) {
		COMPANYCODE = cOMPANYCODE;
	}
	public String getSSNAME() {
		return SSNAME;
	}
	public void setSSNAME(String sSNAME) {
		SSNAME = sSNAME;
	}
	public String getSHAREHDNAME() {
		return SHAREHDNAME;
	}
	public void setSHAREHDNAME(String sHAREHDNAME) {
		SHAREHDNAME = sHAREHDNAME;
	}
	public String getSHAREHDTYPE() {
		return SHAREHDTYPE;
	}
	public void setSHAREHDTYPE(String sHAREHDTYPE) {
		SHAREHDTYPE = sHAREHDTYPE;
	}
	public String getSHARESTYPE() {
		return SHARESTYPE;
	}
	public void setSHARESTYPE(String sHARESTYPE) {
		SHARESTYPE = sHARESTYPE;
	}
	public double getRANK() {
		return RANK;
	}
	public void setRANK(double rANK) {
		RANK = rANK;
	}
	public String getSCODE() {
		return SCODE;
	}
	public void setSCODE(String sCODE) {
		SCODE = sCODE;
	}
	public String getSNAME() {
		return SNAME;
	}
	public void setSNAME(String sNAME) {
		SNAME = sNAME;
	}
	public Date getRDATE() {
		return RDATE;
	}
	public void setRDATE(Date rDATE) {
		RDATE = rDATE;
	}
	public double getSHAREHDNUM() {
		return SHAREHDNUM;
	}
	public void setSHAREHDNUM(double sHAREHDNUM) {
		SHAREHDNUM = sHAREHDNUM;
	}
	public double getLTAG() {
		return LTAG;
	}
	public void setLTAG(double lTAG) {
		LTAG = lTAG;
	}
	public double getZB() {
		return ZB;
	}
	public void setZB(double zB) {
		ZB = zB;
	}
	public Date getNDATE() {
		return NDATE;
	}
	public void setNDATE(Date nDATE) {
		NDATE = nDATE;
	}
	public String getBZ() {
		return BZ;
	}
	public void setBZ(String bZ) {
		BZ = bZ;
	}
	public double getBDBL() {
		return BDBL;
	}
	public void setBDBL(double bDBL) {
		BDBL = bDBL;
	}
	public String getSHAREHDCODE() {
		return SHAREHDCODE;
	}
	public void setSHAREHDCODE(String sHAREHDCODE) {
		SHAREHDCODE = sHAREHDCODE;
	}
	public double getSHAREHDRATIO() {
		return SHAREHDRATIO;
	}
	public void setSHAREHDRATIO(double sHAREHDRATIO) {
		SHAREHDRATIO = sHAREHDRATIO;
	}
	public double getBDSUM() {
		return BDSUM;
	}
	public void setBDSUM(double bDSUM) {
		BDSUM = bDSUM;
	}                

	
	
}
