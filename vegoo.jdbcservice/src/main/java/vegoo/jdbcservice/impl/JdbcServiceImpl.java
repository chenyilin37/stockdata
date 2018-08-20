package vegoo.jdbcservice.impl;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.CallableStatementCallback;
import org.springframework.jdbc.core.CallableStatementCreator;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ParameterizedPreparedStatementSetter;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.SqlParameter;
import org.springframework.jdbc.core.StatementCallback;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.lookup.DataSourceLookupFailureException;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;


import vegoo.jdbcservice.JdbcService;

@Component (
		immediate = true, 
		configurationPid = "stockdata.jdbcservice",
		//service = { PersistentService.class,  ManagedService.class}, 
		property = {
		    //Scheduler.PROPERTY_SCHEDULER_EXPRESSION + "= 0 0 1,18 * * ?", //  静态信息，每天7，8，18抓三次
		    // Scheduler.PROPERTY_SCHEDULER_CONCURRENT + "= false"
		} 
	)
public class JdbcServiceImpl implements JdbcService {

    @Reference(target="(dataSourceName=stockdata)", policy=ReferencePolicy.DYNAMIC)
    volatile private DataSource ds;
    
    private DataSource old_ds;

	//private ComboPooledDataSource cpds;
	private JdbcTemplate _JdbcTemplate;
	
	//private TransactionTemplate transTemplate;

	private JdbcTemplate jdbcTemplate() throws DataAccessException{
		if(ds==null) {
			throw new DataSourceLookupFailureException("没有找到dataSourceName=stockdata数据源");
		}
		
		if(_JdbcTemplate==null || !ds.equals(old_ds)) {
			_JdbcTemplate = new JdbcTemplate(ds);
			old_ds = ds;
		}
		return _JdbcTemplate;
	}	
	

/*	
    public <T> T doInTransaction(DatabaseOperate action) {  
        
        return transTemplate.execute(new TransactionCallback<T>() {  
            public T doInTransaction(TransactionStatus status) {  
                try {  
                	     return action.execute();
                } catch (Exception e) {  
                    status.setRollbackOnly();  
                    e.printStackTrace();  
                }  
                return null;  
            }  
        });  
    }  
  
    public void doInTransactionWithoutResult(DatabaseOperate action) {  
        transTemplate.execute(new TransactionCallbackWithoutResult() {  
            protected void doInTransactionWithoutResult(TransactionStatus status) {  
                try {  
           	       action.execute();
                } catch (Exception e) {  
                	    status.setRollbackOnly();  
                    e.printStackTrace();  
                }  
            }  
        });  
    }  	
    */
	
	
	//-------------------------------------------------------------------------
	// Methods dealing with a plain java.sql.Connection
	//-------------------------------------------------------------------------

	/**
	 * Execute a JDBC data access operation, implemented as callback action
	 * working on a JDBC Connection. This allows for implementing arbitrary
	 * data access operations, within Spring's managed JDBC environment:
	 * that is, participating in Spring-managed transactions and converting
	 * JDBC SQLExceptions into Spring's DataAccessException hierarchy.
	 * <p>The callback action can return a result object, for example a
	 * domain object or a collection of domain objects.
	 * @param action the callback object that specifies the action
	 * @return a result object returned by the action, or {@code null}
	 * @throws DataAccessException if there is any problem
	 */
	public <T> T execute(ConnectionCallback<T> action) throws DataAccessException{
		return jdbcTemplate().execute(action);
	}


	//-------------------------------------------------------------------------
	// Methods dealing with static SQL (java.sql.Statement)
	//-------------------------------------------------------------------------

	/**
	 * Execute a JDBC data access operation, implemented as callback action
	 * working on a JDBC Statement. This allows for implementing arbitrary data
	 * access operations on a single Statement, within Spring's managed JDBC
	 * environment: that is, participating in Spring-managed transactions and
	 * converting JDBC SQLExceptions into Spring's DataAccessException hierarchy.
	 * <p>The callback action can return a result object, for example a
	 * domain object or a collection of domain objects.
	 * @param action callback object that specifies the action
	 * @return a result object returned by the action, or {@code null}
	 * @throws DataAccessException if there is any problem
	 */
	public <T> T execute(StatementCallback<T> action) throws DataAccessException{
		return jdbcTemplate().execute(action);
	}

	/**
	 * Issue a single SQL execute, typically a DDL statement.
	 * @param sql static SQL to execute
	 * @throws DataAccessException if there is any problem
	 */
	public void execute(String sql) throws DataAccessException{
		 jdbcTemplate().execute(sql);
	}

	/**
	 * Execute a query given static SQL, reading the ResultSet with a
	 * ResultSetExtractor.
	 * <p>Uses a JDBC Statement, not a PreparedStatement. If you want to
	 * execute a static query with a PreparedStatement, use the overloaded
	 * {@code query} method with {@code null} as argument array.
	 * @param sql SQL query to execute
	 * @param rse object that will extract all rows of results
	 * @return an arbitrary result object, as returned by the ResultSetExtractor
	 * @throws DataAccessException if there is any problem executing the query
	 * @see #query(String, Object[], ResultSetExtractor)
	 */
	public <T> T query(String sql, ResultSetExtractor<T> rse) throws DataAccessException{
		return jdbcTemplate().query(sql, rse);
	}
	
	
	/**
	 * Execute a query given static SQL, reading the ResultSet on a per-row
	 * basis with a RowCallbackHandler.
	 * <p>Uses a JDBC Statement, not a PreparedStatement. If you want to
	 * execute a static query with a PreparedStatement, use the overloaded
	 * {@code query} method with {@code null} as argument array.
	 * @param sql SQL query to execute
	 * @param rch object that will extract results, one row at a time
	 * @throws DataAccessException if there is any problem executing the query
	 * @see #query(String, Object[], RowCallbackHandler)
	 */
	public void query(String sql, RowCallbackHandler rch) throws DataAccessException{
		 jdbcTemplate().query(sql, rch);
	}
	
	
	/**
	 * Execute a query given static SQL, mapping each row to a Java object
	 * via a RowMapper.
	 * <p>Uses a JDBC Statement, not a PreparedStatement. If you want to
	 * execute a static query with a PreparedStatement, use the overloaded
	 * {@code query} method with {@code null} as argument array.
	 * @param sql SQL query to execute
	 * @param rowMapper object that will map one object per row
	 * @return the result List, containing mapped objects
	 * @throws DataAccessException if there is any problem executing the query
	 * @see #query(String, Object[], RowMapper)
	 */
	public <T> List<T> query(String sql, RowMapper<T> rowMapper) throws DataAccessException{
		return jdbcTemplate().query(sql, rowMapper);
	}

	/**
	 * Execute a query given static SQL, mapping a single result row to a Java
	 * object via a RowMapper.
	 * <p>Uses a JDBC Statement, not a PreparedStatement. If you want to
	 * execute a static query with a PreparedStatement, use the overloaded
	 * {@link #queryForObject(String, RowMapper, Object...)} method with
	 * {@code null} as argument array.
	 * @param sql SQL query to execute
	 * @param rowMapper object that will map one object per row
	 * @return the single mapped object
	 * @throws IncorrectResultSizeDataAccessException if the query does not
	 * return exactly one row
	 * @throws DataAccessException if there is any problem executing the query
	 * @see #queryForObject(String, Object[], RowMapper)
	 */
	public <T> T queryForObject(String sql, RowMapper<T> rowMapper) throws DataAccessException{
		return jdbcTemplate().queryForObject(sql, rowMapper);
	}

	/**
	 * Execute a query for a result object, given static SQL.
	 * <p>Uses a JDBC Statement, not a PreparedStatement. If you want to
	 * execute a static query with a PreparedStatement, use the overloaded
	 * {@link #queryForObject(String, Class, Object...)} method with
	 * {@code null} as argument array.
	 * <p>This method is useful for running static SQL with a known outcome.
	 * The query is expected to be a single row/single column query; the returned
	 * result will be directly mapped to the corresponding object type.
	 * @param sql SQL query to execute
	 * @param requiredType the type that the result object is expected to match
	 * @return the result object of the required type, or {@code null} in case of SQL NULL
	 * @throws IncorrectResultSizeDataAccessException if the query does not return
	 * exactly one row, or does not return exactly one column in that row
	 * @throws DataAccessException if there is any problem executing the query
	 * @see #queryForObject(String, Object[], Class)
	 */
	public <T> T queryForObject(String sql, Class<T> requiredType) throws DataAccessException{
		
		return jdbcTemplate().queryForObject(sql, requiredType);

	}

	/**
	 * Execute a query for a result Map, given static SQL.
	 * <p>Uses a JDBC Statement, not a PreparedStatement. If you want to
	 * execute a static query with a PreparedStatement, use the overloaded
	 * {@link #queryForMap(String, Object...)} method with {@code null}
	 * as argument array.
	 * <p>The query is expected to be a single row query; the result row will be
	 * mapped to a Map (one entry for each column, using the column name as the key).
	 * @param sql SQL query to execute
	 * @return the result Map (one entry for each column, using the
	 * column name as the key)
	 * @throws IncorrectResultSizeDataAccessException if the query does not
	 * return exactly one row
	 * @throws DataAccessException if there is any problem executing the query
	 * @see #queryForMap(String, Object[])
	 * @see ColumnMapRowMapper
	 */
	public Map<String, Object> queryForMap(String sql) throws DataAccessException{
		return jdbcTemplate().queryForMap(sql);
		

	}

	/**
	 * Execute a query for a result list, given static SQL.
	 * <p>Uses a JDBC Statement, not a PreparedStatement. If you want to
	 * execute a static query with a PreparedStatement, use the overloaded
	 * {@code queryForList} method with {@code null} as argument array.
	 * <p>The results will be mapped to a List (one entry for each row) of
	 * result objects, each of them matching the specified element type.
	 * @param sql SQL query to execute
	 * @param elementType the required type of element in the result list
	 * (for example, {@code Integer.class})
	 * @return a List of objects that match the specified element type
	 * @throws DataAccessException if there is any problem executing the query
	 * @see #queryForList(String, Object[], Class)
	 * @see SingleColumnRowMapper
	 */
	public <T> List<T> queryForList(String sql, Class<T> elementType) throws DataAccessException{
		return jdbcTemplate().queryForList(sql, elementType);
		
		

	}

	/**
	 * Execute a query for a result list, given static SQL.
	 * <p>Uses a JDBC Statement, not a PreparedStatement. If you want to
	 * execute a static query with a PreparedStatement, use the overloaded
	 * {@code queryForList} method with {@code null} as argument array.
	 * <p>The results will be mapped to a List (one entry for each row) of
	 * Maps (one entry for each column using the column name as the key).
	 * Each element in the list will be of the form returned by this interface's
	 * queryForMap() methods.
	 * @param sql SQL query to execute
	 * @return an List that contains a Map per row
	 * @throws DataAccessException if there is any problem executing the query
	 * @see #queryForList(String, Object[])
	 */
	public List<Map<String, Object>> queryForList(String sql) throws DataAccessException{
		return jdbcTemplate().queryForList(sql);
		

	}

	/**
	 * Execute a query for a SqlRowSet, given static SQL.
	 * <p>Uses a JDBC Statement, not a PreparedStatement. If you want to
	 * execute a static query with a PreparedStatement, use the overloaded
	 * {@code queryForRowSet} method with {@code null} as argument array.
	 * <p>The results will be mapped to an SqlRowSet which holds the data in a
	 * disconnected fashion. This wrapper will translate any SQLExceptions thrown.
	 * <p>Note that, for the default implementation, JDBC RowSet support needs to
	 * be available at runtime: by default, Sun's {@code com.sun.rowset.CachedRowSetImpl}
	 * class is used, which is part of JDK 1.5+ and also available separately as part of
	 * Sun's JDBC RowSet Implementations download (rowset.jar).
	 * @param sql SQL query to execute
	 * @return a SqlRowSet representation (possibly a wrapper around a
	 * {@code javax.sql.rowset.CachedRowSet})
	 * @throws DataAccessException if there is any problem executing the query
	 * @see #queryForRowSet(String, Object[])
	 * @see SqlRowSetResultSetExtractor
	 * @see javax.sql.rowset.CachedRowSet
	 */
	public SqlRowSet queryForRowSet(String sql) throws DataAccessException{
		return jdbcTemplate().queryForRowSet(sql);
		

	}

	/**
	 * Issue a single SQL update operation (such as an insert, update or delete statement).
	 * @param sql static SQL to execute
	 * @return the number of rows affected
	 * @throws DataAccessException if there is any problem.
	 */
	public int update(String sql) throws DataAccessException{
		return jdbcTemplate().update(sql);
		

	}

	/**
	 * Issue multiple SQL updates on a single JDBC Statement using batching.
	 * <p>Will fall back to separate updates on a single Statement if the JDBC
	 * driver does not support batch updates.
	 * @param sql defining an array of SQL statements that will be executed.
	 * @return an array of the number of rows affected by each statement
	 * @throws DataAccessException if there is any problem executing the batch
	 */
	public int[] batchUpdate(String... sql) throws DataAccessException{
		return jdbcTemplate().batchUpdate(sql);
		

	}


	//-------------------------------------------------------------------------
	// Methods dealing with prepared statements
	//-------------------------------------------------------------------------

	/**
	 * Execute a JDBC data access operation, implemented as callback action
	 * working on a JDBC PreparedStatement. This allows for implementing arbitrary
	 * data access operations on a single Statement, within Spring's managed
	 * JDBC environment: that is, participating in Spring-managed transactions
	 * and converting JDBC SQLExceptions into Spring's DataAccessException hierarchy.
	 * <p>The callback action can return a result object, for example a
	 * domain object or a collection of domain objects.
	 * @param psc object that can create a PreparedStatement given a Connection
	 * @param action callback object that specifies the action
	 * @return a result object returned by the action, or {@code null}
	 * @throws DataAccessException if there is any problem
	 */
	public <T> T execute(PreparedStatementCreator psc, PreparedStatementCallback<T> action) throws DataAccessException{
		return jdbcTemplate().execute(psc, action);
		

	}

	/**
	 * Execute a JDBC data access operation, implemented as callback action
	 * working on a JDBC PreparedStatement. This allows for implementing arbitrary
	 * data access operations on a single Statement, within Spring's managed
	 * JDBC environment: that is, participating in Spring-managed transactions
	 * and converting JDBC SQLExceptions into Spring's DataAccessException hierarchy.
	 * <p>The callback action can return a result object, for example a
	 * domain object or a collection of domain objects.
	 * @param sql SQL to execute
	 * @param action callback object that specifies the action
	 * @return a result object returned by the action, or {@code null}
	 * @throws DataAccessException if there is any problem
	 */
	public <T> T execute(String sql, PreparedStatementCallback<T> action) throws DataAccessException{
		return jdbcTemplate().execute(sql, action);
		

	}

	/**
	 * Query using a prepared statement, reading the ResultSet with a
	 * ResultSetExtractor.
	 * <p>A PreparedStatementCreator can either be implemented directly or
	 * configured through a PreparedStatementCreatorFactory.
	 * @param psc object that can create a PreparedStatement given a Connection
	 * @param rse object that will extract results
	 * @return an arbitrary result object, as returned by the ResultSetExtractor
	 * @throws DataAccessException if there is any problem
	 * @see PreparedStatementCreatorFactory
	 */
	public <T> T query(PreparedStatementCreator psc, ResultSetExtractor<T> rse) throws DataAccessException{
		return jdbcTemplate().query(psc, rse);
		

	}

	/**
	 * Query using a prepared statement, reading the ResultSet with a
	 * ResultSetExtractor.
	 * @param sql SQL query to execute
	 * @param pss object that knows how to set values on the prepared statement.
	 * If this is {@code null}, the SQL will be assumed to contain no bind parameters.
	 * Even if there are no bind parameters, this object may be used to
	 * set fetch size and other performance options.
	 * @param rse object that will extract results
	 * @return an arbitrary result object, as returned by the ResultSetExtractor
	 * @throws DataAccessException if there is any problem
	 */
	public <T> T query(String sql, PreparedStatementSetter pss, ResultSetExtractor<T> rse) throws DataAccessException{
		return jdbcTemplate().query(sql, pss, rse);
		

	}

	/**
	 * Query given SQL to create a prepared statement from SQL and a list
	 * of arguments to bind to the query, reading the ResultSet with a
	 * ResultSetExtractor.
	 * @param sql SQL query to execute
	 * @param args arguments to bind to the query
	 * @param argTypes SQL types of the arguments
	 * (constants from {@code java.sql.Types})
	 * @param rse object that will extract results
	 * @return an arbitrary result object, as returned by the ResultSetExtractor
	 * @throws DataAccessException if the query fails
	 * @see java.sql.Types
	 */
	public <T> T query(String sql, Object[] args, int[] argTypes, ResultSetExtractor<T> rse) throws DataAccessException{
		return jdbcTemplate().query(sql, args, argTypes, rse);
		

	}

	/**
	 * Query given SQL to create a prepared statement from SQL and a list
	 * of arguments to bind to the query, reading the ResultSet with a
	 * ResultSetExtractor.
	 * @param sql SQL query to execute
	 * @param args arguments to bind to the query
	 * (leaving it to the PreparedStatement to guess the corresponding SQL type);
	 * may also contain {@link SqlParameterValue} objects which indicate not
	 * only the argument value but also the SQL type and optionally the scale
	 * @param rse object that will extract results
	 * @return an arbitrary result object, as returned by the ResultSetExtractor
	 * @throws DataAccessException if the query fails
	 */
	public <T> T query(String sql, Object[] args, ResultSetExtractor<T> rse) throws DataAccessException{
		return jdbcTemplate().query(sql, args, rse);
		

	}

	/**
	 * Query given SQL to create a prepared statement from SQL and a list
	 * of arguments to bind to the query, reading the ResultSet with a
	 * ResultSetExtractor.
	 * @param sql SQL query to execute
	 * @param rse object that will extract results
	 * @param args arguments to bind to the query
	 * (leaving it to the PreparedStatement to guess the corresponding SQL type);
	 * may also contain {@link SqlParameterValue} objects which indicate not
	 * only the argument value but also the SQL type and optionally the scale
	 * @return an arbitrary result object, as returned by the ResultSetExtractor
	 * @throws DataAccessException if the query fails
	 */
	public <T> T query(String sql, ResultSetExtractor<T> rse, Object... args) throws DataAccessException{
		return jdbcTemplate().query(sql, rse, args);
		

	}

	/**
	 * Query using a prepared statement, reading the ResultSet on a per-row
	 * basis with a RowCallbackHandler.
	 * <p>A PreparedStatementCreator can either be implemented directly or
	 * configured through a PreparedStatementCreatorFactory.
	 * @param psc object that can create a PreparedStatement given a Connection
	 * @param rch object that will extract results, one row at a time
	 * @throws DataAccessException if there is any problem
	 * @see PreparedStatementCreatorFactory
	 */
	public void query(PreparedStatementCreator psc, RowCallbackHandler rch) throws DataAccessException{
		 jdbcTemplate().query(psc, rch);
		
	}

	/**
	 * Query given SQL to create a prepared statement from SQL and a
	 * PreparedStatementSetter implementation that knows how to bind values
	 * to the query, reading the ResultSet on a per-row basis with a
	 * RowCallbackHandler.
	 * @param sql SQL query to execute
	 * @param pss object that knows how to set values on the prepared statement.
	 * If this is {@code null}, the SQL will be assumed to contain no bind parameters.
	 * Even if there are no bind parameters, this object may be used to
	 * set fetch size and other performance options.
	 * @param rch object that will extract results, one row at a time
	 * @throws DataAccessException if the query fails
	 */
	public void query(String sql, PreparedStatementSetter pss, RowCallbackHandler rch) throws DataAccessException{
		 jdbcTemplate().query(sql, pss, rch);
			
	}
	/**
	 * Query given SQL to create a prepared statement from SQL and a list of
	 * arguments to bind to the query, reading the ResultSet on a per-row basis
	 * with a RowCallbackHandler.
	 * @param sql SQL query to execute
	 * @param args arguments to bind to the query
	 * @param argTypes SQL types of the arguments
	 * (constants from {@code java.sql.Types})
	 * @param rch object that will extract results, one row at a time
	 * @throws DataAccessException if the query fails
	 * @see java.sql.Types
	 */
	public void query(String sql, Object[] args, int[] argTypes, RowCallbackHandler rch) throws DataAccessException{
		 jdbcTemplate().query(sql, args, argTypes, rch);
			
	}

	/**
	 * Query given SQL to create a prepared statement from SQL and a list of
	 * arguments to bind to the query, reading the ResultSet on a per-row basis
	 * with a RowCallbackHandler.
	 * @param sql SQL query to execute
	 * @param args arguments to bind to the query
	 * (leaving it to the PreparedStatement to guess the corresponding SQL type);
	 * may also contain {@link SqlParameterValue} objects which indicate not
	 * only the argument value but also the SQL type and optionally the scale
	 * @param rch object that will extract results, one row at a time
	 * @throws DataAccessException if the query fails
	 */
	public void query(String sql, Object[] args, RowCallbackHandler rch) throws DataAccessException{
		 jdbcTemplate().query(sql, args, rch);
			
	}

	/**
	 * Query given SQL to create a prepared statement from SQL and a list of
	 * arguments to bind to the query, reading the ResultSet on a per-row basis
	 * with a RowCallbackHandler.
	 * @param sql SQL query to execute
	 * @param rch object that will extract results, one row at a time
	 * @param args arguments to bind to the query
	 * (leaving it to the PreparedStatement to guess the corresponding SQL type);
	 * may also contain {@link SqlParameterValue} objects which indicate not
	 * only the argument value but also the SQL type and optionally the scale
	 * @throws DataAccessException if the query fails
	 */
	public void query(String sql, RowCallbackHandler rch, Object... args) throws DataAccessException{
		 jdbcTemplate().query(sql, rch, args);
			
	}

	/**
	 * Query using a prepared statement, mapping each row to a Java object
	 * via a RowMapper.
	 * <p>A PreparedStatementCreator can either be implemented directly or
	 * configured through a PreparedStatementCreatorFactory.
	 * @param psc object that can create a PreparedStatement given a Connection
	 * @param rowMapper object that will map one object per row
	 * @return the result List, containing mapped objects
	 * @throws DataAccessException if there is any problem
	 * @see PreparedStatementCreatorFactory
	 */
	public <T> List<T> query(PreparedStatementCreator psc, RowMapper<T> rowMapper) throws DataAccessException{
		 return jdbcTemplate().query(psc, rowMapper);
		

	}

	/**
	 * Query given SQL to create a prepared statement from SQL and a
	 * PreparedStatementSetter implementation that knows how to bind values
	 * to the query, mapping each row to a Java object via a RowMapper.
	 * @param sql SQL query to execute
	 * @param pss object that knows how to set values on the prepared statement.
	 * If this is {@code null}, the SQL will be assumed to contain no bind parameters.
	 * Even if there are no bind parameters, this object may be used to
	 * set fetch size and other performance options.
	 * @param rowMapper object that will map one object per row
	 * @return the result List, containing mapped objects
	 * @throws DataAccessException if the query fails
	 */
	public <T> List<T> query(String sql, PreparedStatementSetter pss, RowMapper<T> rowMapper) throws DataAccessException{
		 return jdbcTemplate().query(sql, pss, rowMapper);

	}

	/**
	 * Query given SQL to create a prepared statement from SQL and a list
	 * of arguments to bind to the query, mapping each row to a Java object
	 * via a RowMapper.
	 * @param sql SQL query to execute
	 * @param args arguments to bind to the query
	 * @param argTypes SQL types of the arguments
	 * (constants from {@code java.sql.Types})
	 * @param rowMapper object that will map one object per row
	 * @return the result List, containing mapped objects
	 * @throws DataAccessException if the query fails
	 * @see java.sql.Types
	 */
	public <T> List<T> query(String sql, Object[] args, int[] argTypes, RowMapper<T> rowMapper) throws DataAccessException{
		 return jdbcTemplate().query(sql, args, argTypes, rowMapper);
		

	}

	/**
	 * Query given SQL to create a prepared statement from SQL and a list
	 * of arguments to bind to the query, mapping each row to a Java object
	 * via a RowMapper.
	 * @param sql SQL query to execute
	 * @param args arguments to bind to the query
	 * (leaving it to the PreparedStatement to guess the corresponding SQL type);
	 * may also contain {@link SqlParameterValue} objects which indicate not
	 * only the argument value but also the SQL type and optionally the scale
	 * @param rowMapper object that will map one object per row
	 * @return the result List, containing mapped objects
	 * @throws DataAccessException if the query fails
	 */
	public <T> List<T> query(String sql, Object[] args, RowMapper<T> rowMapper) throws DataAccessException{
		 return jdbcTemplate().query(sql, args, rowMapper);
		

	}

	/**
	 * Query given SQL to create a prepared statement from SQL and a list
	 * of arguments to bind to the query, mapping each row to a Java object
	 * via a RowMapper.
	 * @param sql SQL query to execute
	 * @param rowMapper object that will map one object per row
	 * @param args arguments to bind to the query
	 * (leaving it to the PreparedStatement to guess the corresponding SQL type);
	 * may also contain {@link SqlParameterValue} objects which indicate not
	 * only the argument value but also the SQL type and optionally the scale
	 * @return the result List, containing mapped objects
	 * @throws DataAccessException if the query fails
	 */
	public <T> List<T> query(String sql, RowMapper<T> rowMapper, Object... args) throws DataAccessException{
		 return jdbcTemplate().query(sql, rowMapper, args);
		

	}

	/**
	 * Query given SQL to create a prepared statement from SQL and a list
	 * of arguments to bind to the query, mapping a single result row to a
	 * Java object via a RowMapper.
	 * @param sql SQL query to execute
	 * @param args arguments to bind to the query
	 * (leaving it to the PreparedStatement to guess the corresponding SQL type)
	 * @param argTypes SQL types of the arguments
	 * (constants from {@code java.sql.Types})
	 * @param rowMapper object that will map one object per row
	 * @return the single mapped object
	 * @throws IncorrectResultSizeDataAccessException if the query does not
	 * return exactly one row
	 * @throws DataAccessException if the query fails
	 */
	public <T> T queryForObject(String sql, Object[] args, int[] argTypes, RowMapper<T> rowMapper)
			throws DataAccessException{
		 return jdbcTemplate().queryForObject(sql, args, argTypes, rowMapper);
				

			}

	/**
	 * Query given SQL to create a prepared statement from SQL and a list
	 * of arguments to bind to the query, mapping a single result row to a
	 * Java object via a RowMapper.
	 * @param sql SQL query to execute
	 * @param args arguments to bind to the query
	 * (leaving it to the PreparedStatement to guess the corresponding SQL type);
	 * may also contain {@link SqlParameterValue} objects which indicate not
	 * only the argument value but also the SQL type and optionally the scale
	 * @param rowMapper object that will map one object per row
	 * @return the single mapped object
	 * @throws IncorrectResultSizeDataAccessException if the query does not
	 * return exactly one row
	 * @throws DataAccessException if the query fails
	 */
	public <T> T queryForObject(String sql, Object[] args, RowMapper<T> rowMapper) throws DataAccessException{
		 return jdbcTemplate().queryForObject(sql, args, rowMapper);
		

	}

	/**
	 * Query given SQL to create a prepared statement from SQL and a list
	 * of arguments to bind to the query, mapping a single result row to a
	 * Java object via a RowMapper.
	 * @param sql SQL query to execute
	 * @param rowMapper object that will map one object per row
	 * @param args arguments to bind to the query
	 * (leaving it to the PreparedStatement to guess the corresponding SQL type);
	 * may also contain {@link SqlParameterValue} objects which indicate not
	 * only the argument value but also the SQL type and optionally the scale
	 * @return the single mapped object
	 * @throws IncorrectResultSizeDataAccessException if the query does not
	 * return exactly one row
	 * @throws DataAccessException if the query fails
	 */
	public <T> T queryForObject(String sql, RowMapper<T> rowMapper, Object... args) throws DataAccessException{
		 return jdbcTemplate().queryForObject(sql, rowMapper, args);
		

	}

	/**
	 * Query given SQL to create a prepared statement from SQL and a
	 * list of arguments to bind to the query, expecting a result object.
	 * <p>The query is expected to be a single row/single column query; the returned
	 * result will be directly mapped to the corresponding object type.
	 * @param sql SQL query to execute
	 * @param args arguments to bind to the query
	 * @param argTypes SQL types of the arguments
	 * (constants from {@code java.sql.Types})
	 * @param requiredType the type that the result object is expected to match
	 * @return the result object of the required type, or {@code null} in case of SQL NULL
	 * @throws IncorrectResultSizeDataAccessException if the query does not return
	 * exactly one row, or does not return exactly one column in that row
	 * @throws DataAccessException if the query fails
	 * @see #queryForObject(String, Class)
	 * @see java.sql.Types
	 */
	public <T> T queryForObject(String sql, Object[] args, int[] argTypes, Class<T> requiredType)
			throws DataAccessException{
		 return jdbcTemplate().queryForObject(sql, args, argTypes, requiredType);
				

			}

	/**
	 * Query given SQL to create a prepared statement from SQL and a
	 * list of arguments to bind to the query, expecting a result object.
	 * <p>The query is expected to be a single row/single column query; the returned
	 * result will be directly mapped to the corresponding object type.
	 * @param sql SQL query to execute
	 * @param args arguments to bind to the query
	 * (leaving it to the PreparedStatement to guess the corresponding SQL type);
	 * may also contain {@link SqlParameterValue} objects which indicate not
	 * only the argument value but also the SQL type and optionally the scale
	 * @param requiredType the type that the result object is expected to match
	 * @return the result object of the required type, or {@code null} in case of SQL NULL
	 * @throws IncorrectResultSizeDataAccessException if the query does not return
	 * exactly one row, or does not return exactly one column in that row
	 * @throws DataAccessException if the query fails
	 * @see #queryForObject(String, Class)
	 */
	public <T> T queryForObject(String sql, Object[] args, Class<T> requiredType) throws DataAccessException{
		
		 return jdbcTemplate().queryForObject(sql, args, requiredType);

	}

	/**
	 * Query given SQL to create a prepared statement from SQL and a
	 * list of arguments to bind to the query, expecting a result object.
	 * <p>The query is expected to be a single row/single column query; the returned
	 * result will be directly mapped to the corresponding object type.
	 * @param sql SQL query to execute
	 * @param requiredType the type that the result object is expected to match
	 * @param args arguments to bind to the query
	 * (leaving it to the PreparedStatement to guess the corresponding SQL type);
	 * may also contain {@link SqlParameterValue} objects which indicate not
	 * only the argument value but also the SQL type and optionally the scale
	 * @return the result object of the required type, or {@code null} in case of SQL NULL
	 * @throws IncorrectResultSizeDataAccessException if the query does not return
	 * exactly one row, or does not return exactly one column in that row
	 * @throws DataAccessException if the query fails
	 * @see #queryForObject(String, Class)
	 */
	public <T> T queryForObject(String sql, Class<T> requiredType, Object... args) throws DataAccessException{
		 return jdbcTemplate().queryForObject(sql, requiredType, args);

	}

	/**
	 * Query given SQL to create a prepared statement from SQL and a
	 * list of arguments to bind to the query, expecting a result Map.
	 * <p>The query is expected to be a single row query; the result row will be
	 * mapped to a Map (one entry for each column, using the column name as the key).
	 * @param sql SQL query to execute
	 * @param args arguments to bind to the query
	 * @param argTypes SQL types of the arguments
	 * (constants from {@code java.sql.Types})
	 * @return the result Map (one entry for each column, using the
	 * column name as the key)
	 * @throws IncorrectResultSizeDataAccessException if the query does not
	 * return exactly one row
	 * @throws DataAccessException if the query fails
	 * @see #queryForMap(String)
	 * @see ColumnMapRowMapper
	 * @see java.sql.Types
	 */
	public Map<String, Object> queryForMap(String sql, Object[] args, int[] argTypes) throws DataAccessException{
		 return jdbcTemplate().queryForMap(sql, args, argTypes);
		

	}

	/**
	 * Query given SQL to create a prepared statement from SQL and a
	 * list of arguments to bind to the query, expecting a result Map.
	 * The queryForMap() methods defined by this interface are appropriate
	 * when you don't have a domain model. Otherwise, consider using
	 * one of the queryForObject() methods.
	 * <p>The query is expected to be a single row query; the result row will be
	 * mapped to a Map (one entry for each column, using the column name as the key).
	 * @param sql SQL query to execute
	 * @param args arguments to bind to the query
	 * (leaving it to the PreparedStatement to guess the corresponding SQL type);
	 * may also contain {@link SqlParameterValue} objects which indicate not
	 * only the argument value but also the SQL type and optionally the scale
	 * @return the result Map (one entry for each column, using the
	 * column name as the key)
	 * @throws IncorrectResultSizeDataAccessException if the query does not
	 * return exactly one row
	 * @throws DataAccessException if the query fails
	 * @see #queryForMap(String)
	 * @see ColumnMapRowMapper
	 */
	public Map<String, Object> queryForMap(String sql, Object... args) throws DataAccessException{
		 return jdbcTemplate().queryForMap(sql, args);
		

	}

	/**
	 * Query given SQL to create a prepared statement from SQL and a
	 * list of arguments to bind to the query, expecting a result list.
	 * <p>The results will be mapped to a List (one entry for each row) of
	 * result objects, each of them matching the specified element type.
	 * @param sql SQL query to execute
	 * @param args arguments to bind to the query
	 * @param argTypes SQL types of the arguments
	 * (constants from {@code java.sql.Types})
	 * @param elementType the required type of element in the result list
	 * (for example, {@code Integer.class})
	 * @return a List of objects that match the specified element type
	 * @throws DataAccessException if the query fails
	 * @see #queryForList(String, Class)
	 * @see SingleColumnRowMapper
	 */
	public <T>List<T> queryForList(String sql, Object[] args, int[] argTypes, Class<T> elementType)
			throws DataAccessException{
		 return jdbcTemplate().queryForList(sql, args, argTypes, elementType);
				

			}

	/**
	 * Query given SQL to create a prepared statement from SQL and a
	 * list of arguments to bind to the query, expecting a result list.
	 * <p>The results will be mapped to a List (one entry for each row) of
	 * result objects, each of them matching the specified element type.
	 * @param sql SQL query to execute
	 * @param args arguments to bind to the query
	 * (leaving it to the PreparedStatement to guess the corresponding SQL type);
	 * may also contain {@link SqlParameterValue} objects which indicate not
	 * only the argument value but also the SQL type and optionally the scale
	 * @param elementType the required type of element in the result list
	 * (for example, {@code Integer.class})
	 * @return a List of objects that match the specified element type
	 * @throws DataAccessException if the query fails
	 * @see #queryForList(String, Class)
	 * @see SingleColumnRowMapper
	 */
	public <T> List<T> queryForList(String sql, Object[] args, Class<T> elementType) throws DataAccessException{
		 return jdbcTemplate().queryForList(sql, args, elementType);
		

	}

	/**
	 * Query given SQL to create a prepared statement from SQL and a
	 * list of arguments to bind to the query, expecting a result list.
	 * <p>The results will be mapped to a List (one entry for each row) of
	 * result objects, each of them matching the specified element type.
	 * @param sql SQL query to execute
	 * @param elementType the required type of element in the result list
	 * (for example, {@code Integer.class})
	 * @param args arguments to bind to the query
	 * (leaving it to the PreparedStatement to guess the corresponding SQL type);
	 * may also contain {@link SqlParameterValue} objects which indicate not
	 * only the argument value but also the SQL type and optionally the scale
	 * @return a List of objects that match the specified element type
	 * @throws DataAccessException if the query fails
	 * @see #queryForList(String, Class)
	 * @see SingleColumnRowMapper
	 */
	public <T> List<T> queryForList(String sql, Class<T> elementType, Object... args) throws DataAccessException{
		 return jdbcTemplate().queryForList(sql, elementType, args);
		

	}

	/**
	 * Query given SQL to create a prepared statement from SQL and a
	 * list of arguments to bind to the query, expecting a result list.
	 * <p>The results will be mapped to a List (one entry for each row) of
	 * Maps (one entry for each column, using the column name as the key).
	 * Thus  Each element in the list will be of the form returned by this interface's
	 * queryForMap() methods.
	 * @param sql SQL query to execute
	 * @param args arguments to bind to the query
	 * @param argTypes SQL types of the arguments
	 * (constants from {@code java.sql.Types})
	 * @return a List that contains a Map per row
	 * @throws DataAccessException if the query fails
	 * @see #queryForList(String)
	 * @see java.sql.Types
	 */
	public List<Map<String, Object>> queryForList(String sql, Object[] args, int[] argTypes) throws DataAccessException{
		 return jdbcTemplate().queryForList(sql, args, argTypes);
		

	}

	/**
	 * Query given SQL to create a prepared statement from SQL and a
	 * list of arguments to bind to the query, expecting a result list.
	 * <p>The results will be mapped to a List (one entry for each row) of
	 * Maps (one entry for each column, using the column name as the key).
	 * Each element in the list will be of the form returned by this interface's
	 * queryForMap() methods.
	 * @param sql SQL query to execute
	 * @param args arguments to bind to the query
	 * (leaving it to the PreparedStatement to guess the corresponding SQL type);
	 * may also contain {@link SqlParameterValue} objects which indicate not
	 * only the argument value but also the SQL type and optionally the scale
	 * @return a List that contains a Map per row
	 * @throws DataAccessException if the query fails
	 * @see #queryForList(String)
	 */
	public List<Map<String, Object>> queryForList(String sql, Object... args) throws DataAccessException{
		 return jdbcTemplate().queryForList(sql, args);
		

	}

	/**
	 * Query given SQL to create a prepared statement from SQL and a
	 * list of arguments to bind to the query, expecting a SqlRowSet.
	 * <p>The results will be mapped to an SqlRowSet which holds the data in a
	 * disconnected fashion. This wrapper will translate any SQLExceptions thrown.
	 * <p>Note that that, for the default implementation, JDBC RowSet support needs to
	 * be available at runtime: by default, Sun's {@code com.sun.rowset.CachedRowSetImpl}
	 * class is used, which is part of JDK 1.5+ and also available separately as part of
	 * Sun's JDBC RowSet Implementations download (rowset.jar).
	 * @param sql SQL query to execute
	 * @param args arguments to bind to the query
	 * @param argTypes SQL types of the arguments
	 * (constants from {@code java.sql.Types})
	 * @return a SqlRowSet representation (possibly a wrapper around a
	 * {@code javax.sql.rowset.CachedRowSet})
	 * @throws DataAccessException if there is any problem executing the query
	 * @see #queryForRowSet(String)
	 * @see SqlRowSetResultSetExtractor
	 * @see javax.sql.rowset.CachedRowSet
	 * @see java.sql.Types
	 */
	public SqlRowSet queryForRowSet(String sql, Object[] args, int[] argTypes) throws DataAccessException{
		 return jdbcTemplate().queryForRowSet(sql, args, argTypes);
		

	}

	/**
	 * Query given SQL to create a prepared statement from SQL and a
	 * list of arguments to bind to the query, expecting a SqlRowSet.
	 * <p>The results will be mapped to an SqlRowSet which holds the data in a
	 * disconnected fashion. This wrapper will translate any SQLExceptions thrown.
	 * <p>Note that that, for the default implementation, JDBC RowSet support needs to
	 * be available at runtime: by default, Sun's {@code com.sun.rowset.CachedRowSetImpl}
	 * class is used, which is part of JDK 1.5+ and also available separately as part of
	 * Sun's JDBC RowSet Implementations download (rowset.jar).
	 * @param sql SQL query to execute
	 * @param args arguments to bind to the query
	 * (leaving it to the PreparedStatement to guess the corresponding SQL type);
	 * may also contain {@link SqlParameterValue} objects which indicate not
	 * only the argument value but also the SQL type and optionally the scale
	 * @return a SqlRowSet representation (possibly a wrapper around a
	 * {@code javax.sql.rowset.CachedRowSet})
	 * @throws DataAccessException if there is any problem executing the query
	 * @see #queryForRowSet(String)
	 * @see SqlRowSetResultSetExtractor
	 * @see javax.sql.rowset.CachedRowSet
	 */
	public SqlRowSet queryForRowSet(String sql, Object... args) throws DataAccessException{
		 return jdbcTemplate().queryForRowSet(sql, args);
		

	}

	/**
	 * Issue a single SQL update operation (such as an insert, update or delete statement)
	 * using a PreparedStatementCreator to provide SQL and any required parameters.
	 * <p>A PreparedStatementCreator can either be implemented directly or
	 * configured through a PreparedStatementCreatorFactory.
	 * @param psc object that provides SQL and any necessary parameters
	 * @return the number of rows affected
	 * @throws DataAccessException if there is any problem issuing the update
	 * @see PreparedStatementCreatorFactory
	 */
	public int update(PreparedStatementCreator psc) throws DataAccessException{
		 return jdbcTemplate().update(psc);
		

	}

	/**
	 * Issue an update statement using a PreparedStatementCreator to provide SQL and
	 * any required parameters. Generated keys will be put into the given KeyHolder.
	 * <p>Note that the given PreparedStatementCreator has to create a statement
	 * with activated extraction of generated keys (a JDBC 3.0 feature). This can
	 * either be done directly or through using a PreparedStatementCreatorFactory.
	 * @param psc object that provides SQL and any necessary parameters
	 * @param generatedKeyHolder KeyHolder that will hold the generated keys
	 * @return the number of rows affected
	 * @throws DataAccessException if there is any problem issuing the update
	 * @see PreparedStatementCreatorFactory
	 * @see org.springframework.jdbc.support.GeneratedKeyHolder
	 */
	public int update(PreparedStatementCreator psc, KeyHolder generatedKeyHolder) throws DataAccessException{
		 return jdbcTemplate().update(psc, generatedKeyHolder);
		

	}

	/**
	 * Issue an update statement using a PreparedStatementSetter to set bind parameters,
	 * with given SQL. Simpler than using a PreparedStatementCreator as this method
	 * will create the PreparedStatement: The PreparedStatementSetter just needs to
	 * set parameters.
	 * @param sql SQL containing bind parameters
	 * @param pss helper that sets bind parameters. If this is {@code null}
	 * we run an update with static SQL.
	 * @return the number of rows affected
	 * @throws DataAccessException if there is any problem issuing the update
	 */
	public int update(String sql, PreparedStatementSetter pss) throws DataAccessException{
		 return jdbcTemplate().update(sql, pss);
		

	}

	/**
	 * Issue a single SQL update operation (such as an insert, update or delete statement)
	 * via a prepared statement, binding the given arguments.
	 * @param sql SQL containing bind parameters
	 * @param args arguments to bind to the query
	 * @param argTypes SQL types of the arguments
	 * (constants from {@code java.sql.Types})
	 * @return the number of rows affected
	 * @throws DataAccessException if there is any problem issuing the update
	 * @see java.sql.Types
	 */
	public int update(String sql, Object[] args, int[] argTypes) throws DataAccessException{
		 return jdbcTemplate().update(sql, args, argTypes);
		

	}

	/**
	 * Issue a single SQL update operation (such as an insert, update or delete statement)
	 * via a prepared statement, binding the given arguments.
	 * @param sql SQL containing bind parameters
	 * @param args arguments to bind to the query
	 * (leaving it to the PreparedStatement to guess the corresponding SQL type);
	 * may also contain {@link SqlParameterValue} objects which indicate not
	 * only the argument value but also the SQL type and optionally the scale
	 * @return the number of rows affected
	 * @throws DataAccessException if there is any problem issuing the update
	 */
	public int update(String sql, Object... args) throws DataAccessException{
		 return jdbcTemplate().update(sql, args);
		

	}

	/**
	 * Issue multiple update statements on a single PreparedStatement,
	 * using batch updates and a BatchPreparedStatementSetter to set values.
	 * <p>Will fall back to separate updates on a single PreparedStatement
	 * if the JDBC driver does not support batch updates.
	 * @param sql defining PreparedStatement that will be reused.
	 * All statements in the batch will use the same SQL.
	 * @param pss object to set parameters on the PreparedStatement
	 * created by this method
	 * @return an array of the number of rows affected by each statement
	 * @throws DataAccessException if there is any problem issuing the update
	 */
	public int[] batchUpdate(String sql, BatchPreparedStatementSetter pss) throws DataAccessException{
		 return jdbcTemplate().batchUpdate(sql, pss);
		

	}

	/**
	 * Execute a batch using the supplied SQL statement with the batch of supplied arguments.
	 * @param sql the SQL statement to execute
	 * @param batchArgs the List of Object arrays containing the batch of arguments for the query
	 * @return an array containing the numbers of rows affected by each update in the batch
	 */
	public int[] batchUpdate(String sql, List<Object[]> batchArgs) throws DataAccessException{
		 return jdbcTemplate().batchUpdate(sql, batchArgs);
		

	}

	/**
	 * Execute a batch using the supplied SQL statement with the batch of supplied arguments.
	 * @param sql the SQL statement to execute.
	 * @param batchArgs the List of Object arrays containing the batch of arguments for the query
	 * @param argTypes SQL types of the arguments
	 * (constants from {@code java.sql.Types})
	 * @return an array containing the numbers of rows affected by each update in the batch
	 */
	public int[] batchUpdate(String sql, List<Object[]> batchArgs, int[] argTypes) throws DataAccessException{
		 return jdbcTemplate().batchUpdate(sql, batchArgs, argTypes);
		

	}

	/**
	 * Execute multiple batches using the supplied SQL statement with the collect of supplied arguments.
	 * The arguments' values will be set using the ParameterizedPreparedStatementSetter.
	 * Each batch should be of size indicated in 'batchSize'.
	 * @param sql the SQL statement to execute.
	 * @param batchArgs the List of Object arrays containing the batch of arguments for the query
	 * @param batchSize batch size
	 * @param pss ParameterizedPreparedStatementSetter to use
	 * @return an array containing for each batch another array containing the numbers of rows affected
	 * by each update in the batch
	 */
	public <T> int[][] batchUpdate(String sql, Collection<T> batchArgs, int batchSize,
			ParameterizedPreparedStatementSetter<T> pss) throws DataAccessException{
		 return jdbcTemplate().batchUpdate(sql, batchArgs, batchSize, pss);
	}

	//-------------------------------------------------------------------------
	// Methods dealing with callable statements
	//-------------------------------------------------------------------------

	/**
	 * Execute a JDBC data access operation, implemented as callback action
	 * working on a JDBC CallableStatement. This allows for implementing arbitrary
	 * data access operations on a single Statement, within Spring's managed
	 * JDBC environment: that is, participating in Spring-managed transactions
	 * and converting JDBC SQLExceptions into Spring's DataAccessException hierarchy.
	 * <p>The callback action can return a result object, for example a
	 * domain object or a collection of domain objects.
	 * @param csc object that can create a CallableStatement given a Connection
	 * @param action callback object that specifies the action
	 * @return a result object returned by the action, or {@code null}
	 * @throws DataAccessException if there is any problem
	 */
	public <T> T execute(CallableStatementCreator csc, CallableStatementCallback<T> action) throws DataAccessException{
		 return jdbcTemplate().execute(csc, action);
		

	}

	/**
	 * Execute a JDBC data access operation, implemented as callback action
	 * working on a JDBC CallableStatement. This allows for implementing arbitrary
	 * data access operations on a single Statement, within Spring's managed
	 * JDBC environment: that is, participating in Spring-managed transactions
	 * and converting JDBC SQLExceptions into Spring's DataAccessException hierarchy.
	 * <p>The callback action can return a result object, for example a
	 * domain object or a collection of domain objects.
	 * @param callString the SQL call string to execute
	 * @param action callback object that specifies the action
	 * @return a result object returned by the action, or {@code null}
	 * @throws DataAccessException if there is any problem
	 */
	public <T> T execute(String callString, CallableStatementCallback<T> action) throws DataAccessException{
		 return jdbcTemplate().execute(callString, action);
		

	}

	/**
	 * Execute a SQL call using a CallableStatementCreator to provide SQL and any
	 * required parameters.
	 * @param csc object that provides SQL and any necessary parameters
	 * @param declaredParameters list of declared SqlParameter objects
	 * @return Map of extracted out parameters
	 * @throws DataAccessException if there is any problem issuing the update
	 */
	public Map<String, Object> call(CallableStatementCreator csc, List<SqlParameter> declaredParameters)
			throws DataAccessException{
				
		 return jdbcTemplate().call(csc, declaredParameters);

	}

	
}
