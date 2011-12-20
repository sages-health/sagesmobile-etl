/**
 * 
 */
package org.jhuapl.edu.sages.etl.strategy;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Savepoint;

import org.apache.log4j.Logger;
import org.jhuapl.edu.sages.etl.SagesEtlException;
import org.postgresql.util.PSQLException;

/**
 * @author POKUAM1
 * @created Dec 19, 2011
 */
public class ETLCleanupStrategy extends ETLPostgresqlStrategy {

	private static final Logger log = Logger.getLogger(ETLPostgresqlStrategy.class);
	private SagesOpenCsvJar m_socj;
	
	/* (non-Javadoc)
	 * @see org.jhuapl.edu.sages.etl.ETLStrategy#alterCleanseTableAddFlagColumn(java.sql.Connection, java.sql.Savepoint, java.sql.Savepoint)
	 */
	@Override
	public void alterCleanseTableAddFlagColumn(Connection c, Savepoint save1, Savepoint createCleanseSavepoint)
			throws SQLException, SagesEtlException {
		String sqlaltertableAddColumn = addFlagColumn(m_socj.src_table_name);
	    PreparedStatement PS_addcolumn_Flag = c.prepareStatement(sqlaltertableAddColumn);
	    log.info("ALTER STATEMENT: " + sqlaltertableAddColumn);

	    try {
	    	PS_addcolumn_Flag.execute();
	    } catch (PSQLException e){
	    	if (isSqlStateIgnorable(e)){
	    		/** catches that 'etl_flag' column already exists **/
	    		/** known error. we can ignore & recover. **/
	    		log.info("Safe to ignore, this is expected:" + e.getSQLState() + ", " + e.getMessage());
	    		c.rollback(createCleanseSavepoint);	    		
	    	} else {
	    		/** unknown error. bad. must abort. **/
	    		log.fatal("Uh-oh, something bad happened trying to build the ETL_CLEANSING_TABLE. Starting error cleanup.", e); 
	    		errorCleanup(m_socj, save1, c, m_socj.getCurrentFile(), m_socj.getFaileddir_csvfiles(), e);
	    	}

	    } catch (SQLException e){ //TODO MS Access specific
	    	if ("S0021".equals(e.getSQLState())){
	    		log.debug("ETL_LOGGER:" + e.getSQLState() + ", " + e.getMessage()); //TODO: LOGGING
	    	} else {
	    		errorCleanup(m_socj, save1, c, null, m_socj.getFaileddir_csvfiles(), e);
	    		throw SagesOpenCsvJar.abort("Uh-oh, something happened trying to add column etl_flag to ETL_CLEANSING_TABLE.", e); 
	    	}
	    } catch (Exception e) {
	    	errorCleanup(m_socj, save1, c, null,  m_socj.getFaileddir_csvfiles(), e);
	    	throw SagesOpenCsvJar.abort("Uh-oh, something happened trying to add column etl_flag to ETL_CLEANSING_TABLE.", e); 
	    }
	}


	
//	/**
//	 * @param e
//	 * @return
//	 */
//	protected boolean isSqlStateIgnorable(PSQLException e) {
//		return ignorableErrorCodes.contains(e.getSQLState());
//	}

	
}
