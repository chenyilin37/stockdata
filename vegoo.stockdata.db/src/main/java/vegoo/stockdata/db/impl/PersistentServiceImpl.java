package vegoo.stockdata.db.impl;

import java.beans.PropertyVetoException;
import java.util.Dictionary;

import javax.sql.DataSource;

import org.osgi.service.cm.ConfigurationException;
import org.osgi.service.cm.ManagedService;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferencePolicy;

import vegoo.jdbcservice.JdbcService;
import vegoo.redis.RedisService;
import vegoo.stockdata.db.PersistentService;

public class PersistentServiceImpl{

}