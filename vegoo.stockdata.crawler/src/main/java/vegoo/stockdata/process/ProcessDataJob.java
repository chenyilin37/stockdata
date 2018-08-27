package vegoo.stockdata.process;

import java.math.BigDecimal;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.Dictionary;
import java.util.List;
import java.util.Map;

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

import vegoo.jdbcservice.JdbcService;
import vegoo.stockdata.core.BaseJob;
import vegoo.stockdata.db.GdhsPersistentService;
import vegoo.stockdata.db.JgccPersistentService;
import vegoo.stockdata.db.JgccmxPersistentService;


@Component (
	immediate = true, 
	configurationPid = "stockdata.processdata",
	property = {
	    Scheduler.PROPERTY_SCHEDULER_EXPRESSION + "= 0 0 */1 * * ?",   // 静态信息，每天7，8，18抓三次
	} 
) 
public class ProcessDataJob extends BaseJob implements Job, ManagedService{
	private static final Logger logger = LoggerFactory.getLogger(ProcessDataJob.class);

	private static final String PN_BLOCKED   = "blocked";

    @Reference private JdbcService db;

    @Reference private GdhsPersistentService dbGdhs;
    @Reference private JgccPersistentService dbJgcc;
    @Reference private JgccmxPersistentService dbJgccmx;

    private boolean blocked = false;
    
    @Override
	public void updated(Dictionary<String, ?> properties) throws ConfigurationException {
    	String pv = (String) properties.get(PN_BLOCKED);
    	blocked = "true".equalsIgnoreCase(pv) ;
    }

	@Override
	protected void executeJob(JobContext context) {
		if(blocked) {
			return;
		}
		
		dbGdhs.settleGdhs();
		dbJgcc.settleJgcc();
		dbJgccmx.settleJgccmx();
	}



}
