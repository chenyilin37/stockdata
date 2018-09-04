package vegoo.stockdata.db.fhsg;

import java.math.BigDecimal;
import java.sql.Types;
import java.util.Date;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.osgi.service.cm.ConfigurationException;
import org.osgi.service.cm.ManagedService;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.EmptyResultDataAccessException;

import vegoo.jdbcservice.JdbcService;
import vegoo.redis.RedisService;
import vegoo.stockdata.core.model.FhsgItemDao;
import vegoo.stockdata.core.utils.StockUtil;
import vegoo.stockdata.db.FhsgPersistentService;
import vegoo.stockdata.db.base.PersistentServiceImpl;

@Component (immediate = true)
public class FhsgPersistentServiceImpl extends PersistentServiceImpl implements FhsgPersistentService, ManagedService {
	private static final Logger logger = LoggerFactory.getLogger(FhsgPersistentServiceImpl.class);

    @Reference private RedisService redis;
    @Reference private JdbcService db;

	private static final Map<String,Map<Date,FhsgItemDao>> cqcxCache = new HashMap<>();

	@Override
	public void updated(Dictionary<String, ?> properties) throws ConfigurationException {
		
	}
 	
 	@Activate
 	public void activate() {
 		loadCqcxData();
 	}
 	
	private void loadCqcxData() {
		
		
	}

	private static final String SQL_EXIST_FHSG = "select id from fhsg where scode=? and rdate=?";
	public boolean existFhsg(String stockCode, Date rDate) {
		try {
			Integer val = db.queryForObject(SQL_EXIST_FHSG, new Object[] {stockCode, rDate}, 
					new int[] {Types.VARCHAR, Types.DATE}, Integer.class);

			return  val != null;
		}catch(EmptyResultDataAccessException e) {
			return false;
		}catch(Exception e) {
    		logger.error(String.format("stockCode:%s, endDate:%tF%n", stockCode, rDate), e);
			return false;
		}
	}
	
	private static final String SQL_ADD_FHSG = "insert into fhsg(SCode,Rdate,SZZBL,SGBL,ZGBL,XJFH,GXL,YAGGR,GQDJR,CQCXR,YAGGRHSRZF,GQDJRQSRZF,CQCXRHSSRZF,TotalEquity,"
			+ "EarningsPerShare,NetAssetsPerShare,MGGJJ,MGWFPLY,JLYTBZZ,ResultsbyDate,ProjectProgress,AllocationPlan,YCQTS) "
			+ "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
	
	@Override
	public void insertFhsg(FhsgItemDao dao) {
		try {
		   db.update( SQL_ADD_FHSG, new Object[] {dao.getSCode(), dao.getRDate(),
				dao.getSZZBL(), dao.getSGBL(), dao.getZGBL(), dao.getXJFH(), dao.getGXL(),
				dao.getYAGGR(), dao.getGQDJR(), dao.getCQCXR(),
				dao.getYAGGRHSRZF(), dao.getGQDJRQSRZF(), dao.getCQCXRHSSRZF(),
				dao.getTotalEquity(), dao.getEarningsPerShare(), dao.getNetAssetsPerShare(),
				dao.getMGGJJ(), dao.getMGWFPLY(), dao.getJLYTBZZ(), dao.getResultsbyDate(),
				dao.getProjectProgress(), dao.getAllocationPlan(), dao.getYCQTS()}, 
				new int[] {Types.VARCHAR, Types.DATE,  
						   Types.DOUBLE,Types.DOUBLE,Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, 
						   Types.DATE,  Types.DATE, Types.DATE,
						   Types.DOUBLE, Types.DOUBLE, Types.DOUBLE,Types.DOUBLE,Types.DOUBLE,
						   Types.DOUBLE, Types.DOUBLE, Types.DOUBLE,Types.DOUBLE,
						   Types.DATE,Types.VARCHAR, Types.VARCHAR,Types.DOUBLE});  
		}catch(Exception e) {
			logger.error("", e);
		}
   }


	private static final String QRY_CQCX = "SELECT szzbl FROM fhsg where SCode=? and (CQCXR>? and CQCXR<=?) and szzbl != 0";
	@Override
	public double adjustWithHoldNum(Date reportDate, String scode, double hdNum) {
		Date prevRDate = StockUtil.getReportDate(reportDate, -1);
		
		try {
			List<Map<String,Object>> cqcxItems = db.queryForList(QRY_CQCX,
					new Object[] {scode, prevRDate, reportDate},
					new int[] {Types.VARCHAR, Types.DATE, Types.DATE});
			if(!cqcxItems.isEmpty()) {
				return adjustWithHoldNum(hdNum, cqcxItems);
			}
		}catch(Exception e) {
			logger.error("",e);
		}
		
		return hdNum;
	}
	
	private double adjustWithHoldNum(double hdNum, List<Map<String, Object>> cqcxItems) {
		for(Map<String, Object> item: cqcxItems) {
			BigDecimal szzbl = (BigDecimal) item.get("szzbl");
			hdNum *= (1.0 + szzbl.doubleValue()/10);
		}
		return Math.round(hdNum);
	}

	private static FhsgItemDao getFhsgItemDao(String stockcode, Date tdate) {
		Map<Date, FhsgItemDao>  items = cqcxCache.get(stockcode);
		if(items==null) {
			return null;
		}
		return items.get(tdate);
	}
	
	@Override
	public double calcLClose(String stockcode, Date transdate, double lClose) {
		
		FhsgItemDao cqcxItem = getFhsgItemDao(stockcode,transdate);
		if(cqcxItem==null) {
			return lClose;
		}
		return calcLClose(lClose, cqcxItem);
	}

	
	private double calcLClose(double lClose, FhsgItemDao cqcxItem ) {
		
		return 0;
	}

	public static void main(String[] args) {
		double hdNum = 8674096;
		hdNum *= (1.0 + 6.0/10);
		System.out.println(hdNum);
	}

	
}
