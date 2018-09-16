package vegoo.stockdata.process;

import java.math.BigDecimal;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.Dictionary;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import org.apache.karaf.scheduler.Job;
import org.apache.karaf.scheduler.JobContext;
import org.apache.karaf.scheduler.Scheduler;
import org.osgi.service.cm.ConfigurationException;
import org.osgi.service.cm.ManagedService;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.EmptyResultDataAccessException;

import vegoo.stockdata.core.BaseJob;
import vegoo.stockdata.db.FhsgPersistentService;
import vegoo.stockdata.db.GdhsPersistentService;
import vegoo.stockdata.db.JgccPersistentService;
import vegoo.stockdata.db.JgccmxPersistentService;
import vegoo.stockdata.db.SdltgdPersistentService;


@Component (
	immediate = true, 
	configurationPid = "stockdata.processdata",
	service = { Job.class,  ManagedService.class},
	property = {
	    Scheduler.PROPERTY_SCHEDULER_EXPRESSION + "= 0 * */1 * * ?",   // 静态信息，每天7，8，18抓三次
	} 
) 
public class ProcessDataJob extends BaseJob implements Job, ManagedService{
	private static final Logger logger = LoggerFactory.getLogger(ProcessDataJob.class);

	private static final String PN_BLOCKED   = "blocked";
	private static final String PN_RESET_GDHS   = "reset-gdhs";
	private static final String PN_RESET_JGCC   = "reset-jgcc";
	private static final String PN_RESET_JGCCMX   = "reset-jgccmx";
	private static final String PN_RESET_SDLTGD   = "reset-sdltgd";
	private static final String PN_RESET_FHSG   = "reset-fhsg";

    @Reference private GdhsPersistentService dbGdhs;
    @Reference private JgccPersistentService dbJgcc;
    @Reference private JgccmxPersistentService dbJgccmx;
    @Reference private SdltgdPersistentService dbSdltgd;
    @Reference private FhsgPersistentService dbFhsg;
    
    private boolean blocked = false;
    
    private boolean resetGdhs = false;
    private boolean resetJgcc = false;
    private boolean resetJgccmx = false;
    private boolean resetSdltgd = false;
    private boolean resetFhsg = false;
 

	private Future<?> futureGdhs;
	private Future<?> futureJgcc;
	private Future<?> futureJgccmx;
	private Future<?> futureSdltgd;
	private Future<?> futureFhsg;
    
    @Override
	public void updated(Dictionary<String, ?> properties) throws ConfigurationException {
		/* ！！！本函数内不要做需要长时间才能完成的工作，否则，会影响其他BUNDLE的初始化！！！  */

		blocked = "true".equalsIgnoreCase((String) properties.get(PN_BLOCKED)) ;
		
		resetGdhs = "true".equalsIgnoreCase((String) properties.get(PN_RESET_GDHS)) ;
		resetJgcc = "true".equalsIgnoreCase((String) properties.get(PN_RESET_JGCC)) ;
		resetJgccmx = "true".equalsIgnoreCase((String) properties.get(PN_RESET_JGCCMX)) ;
		resetSdltgd = "true".equalsIgnoreCase((String) properties.get(PN_RESET_SDLTGD)) ;
		resetFhsg = "true".equalsIgnoreCase((String) properties.get(PN_RESET_FHSG)) ;
				
    }

	@Override
	protected void executeJob(JobContext context) {
		if(blocked) {
			return;
		}
		
		if((futureFhsg == null) || (futureFhsg.isCancelled()||futureFhsg.isDone())){
			processFhsg();
		}
		
		if((futureJgccmx == null) || (futureJgccmx.isCancelled()||futureJgccmx.isDone())){
			processJgccmx();
		}

		if((futureJgcc == null) || (futureJgcc.isCancelled()||futureJgcc.isDone())){
			processJgcc();
		}
		
		if((futureSdltgd == null) || (futureSdltgd.isCancelled()||futureSdltgd.isDone())){
			processSdltgd();
		}
		
		if((futureGdhs == null) || (futureGdhs.isCancelled()||futureGdhs.isDone())){
			processGdhs();
		}
	}
	
	private void processFhsg() {
		futureFhsg = asyncExecute(new Runnable() {
			@Override
			public void run() {
				logger.info("process Fhsg.....");
				try {
					dbFhsg.settleFhsg(resetFhsg);
				}finally {
					futureFhsg = null;
					resetFhsg=false;
				}
			}}) ;				
	}

	private void processGdhs() {
		futureGdhs = asyncExecute(new Runnable() {
			@Override
			public void run() {
				logger.info("process Gdhs.....");
				try {
					dbGdhs.settleGdhs(resetGdhs);
				}finally {
					futureGdhs = null;
					resetGdhs = false;
				}
			}}) ;		
	}

	private void processSdltgd() {
		futureSdltgd = asyncExecute(new Runnable() {
			@Override
			public void run() {
				logger.info("process Sdltgd.....");
				try {
					dbSdltgd.settleSdltgd(resetSdltgd);
				}finally{
					futureSdltgd = null;
					resetSdltgd = false;
				}
			}}) ;
	}

	private void processJgccmx() {
		futureJgccmx = asyncExecute(new Runnable() {
			@Override
			public void run() {
				logger.info("process JgccMX.....");
				try {
					dbJgccmx.settleJgccmx(resetJgccmx);
				}finally {
					futureJgccmx = null;
					resetJgccmx = false;
				}
			}}) ;		
	}

	private void processJgcc() {
		futureJgcc = asyncExecute(new Runnable() {
			@Override
			public void run() {
				logger.info("process Jgcc.....");
				try {
					dbJgcc.settleJgcc(resetJgcc);
				}finally {
					futureJgcc = null;
					resetJgcc = false;
				}
			}}) ;
	}

}
