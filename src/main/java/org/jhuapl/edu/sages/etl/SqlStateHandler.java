/**
 * 
 */
package org.jhuapl.edu.sages.etl;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.jhuapl.edu.sages.etl.opencsvpods.DumbTestOpenCsvJar;
import org.jhuapl.edu.sages.etl.strategy.SagesOpenCsvJar;
import org.postgresql.util.PSQLException;

/**
 * Handles the various exceptions that result from SQL activity
 * 
 * @author POKUAM1
 * @created Dec 20, 2011
 */
public class SqlStateHandler {
	private static final Logger log = Logger.getLogger(SqlStateHandler.class);
	
	/**
	 * SQL codes that are safe to ignore and are expected
	 */
	protected Set<String> ignorableErrorCodes = new HashSet<String>(){{
		add("CODE");
	}};

	/**
	 * default constructor
	 */
	public SqlStateHandler() {
	}
	
	
	public  Set<String> getIgnorableErrorCodes() {
		return ignorableErrorCodes;
	}

	public void setIgnorableErrorCodes(Set<String> ignorableErrorCodes) {
		this.ignorableErrorCodes = ignorableErrorCodes;
	}
	
	/**
	 * @param e {@link SQLException}
	 * @return boolean if SQL state code is in the ignorable set of codes
	 */
	protected boolean isSqlStateIgnorable(SQLException e) {
		return getIgnorableErrorCodes().contains(e.getSQLState());
	}
	
	
	/**
	 * @param c - {@link Connection}
	 * @param socj - {@link SagesOpenCsvJar}
	 * @param save1 - {@link Savepoint}
	 * @param createCleanseSavepoint - {@link Savepoint}
	 * @throws SQLException
	 * @throws SagesEtlException
	 */
	public void sqlExceptionHandlerBuildCleanseTable(Connection c,
			SagesOpenCsvJar socj, Savepoint save1,
			Savepoint createCleanseSavepoint, Exception ex) throws SQLException,
			SagesEtlException {
		
		try {
			throw ex;
	    } catch (PSQLException e){ //TODO: make this generic for SQLException
	    	
	    	String infoMsg = "Safe to ignore, this is expected:" + e.getSQLState() + ", " + e.getMessage();
	    	String fatalMsg = "Uh-oh, something bad happened trying to build the ETL_CLEANSING_TABLE. Starting error cleanup."; 
	    	sqlStateHandler(c, socj, save1, createCleanseSavepoint, e, infoMsg, fatalMsg);
	    	
					//	    	if (isSqlStateIgnorable(e)){
					//	    		/** known error. we can ignore & recover. **/
					//	    		log.info("Safe to ignore, this is expected:" + e.getSQLState() + ", " + e.getMessage()); //TODO: LOGGING
					//	    		c.rollback(createCleanseSavepoint);
					//	    		
					//	    	} else {
					//	    		/** unknown error. bad. must abort. **/
					//	    		log.fatal("Uh-oh, something bad happened trying to build the ETL_CLEANSING_TABLE. Starting error cleanup.", e); 
					//	    		errorCleanup(m_socj, save1, c, m_socj.currentFile, socj.faileddir_csvfiles, e);
					//	    	}
	    	
	    } catch (SQLException e){ //TODO: make this generic for SQLException this is MS Access error
	    	if ("S0001".equals(e.getSQLState())){
	    		log.debug("ETL_LOGGER:" + e.getSQLState() + ", " + e.getMessage()); //TODO: LOGGING
	    	} else {
	    		errorCleanup(socj, save1, c, socj.getCurrentFile(), socj.getFaileddir_csvfiles(), e);
	    		throw SagesOpenCsvJar.abort("Uh-oh, something happened trying to build the ETL_CLEANSING_TABLE.", e); 
	    	}
	    } catch (Exception e) {
	    	errorCleanup(socj, save1, c, socj.getCurrentFile(), socj.getFaileddir_csvfiles(), e);
	    	throw SagesOpenCsvJar.abort("Uh-oh, something happened trying to build the ETL_CLEANSING_TABLE.", e); 
	    }
	}
	
	/**
	 * @param c - {@link Connection}
	 * @param save1 - {@link Savepoint}
	 * @param createCleanseSavepoint - {@link Savepoint}
	 * @throws SQLException
	 * @throws SagesEtlException
	 */
	public void sqlExceptionHandlerAlterCleanseTableAddFlagColumn(Connection c, SagesOpenCsvJar socj, Savepoint save1,
					Savepoint createCleanseSavepoint, Exception ex) 
					throws SQLException, SagesEtlException {
		
		try {
			throw ex;
	    } catch (PSQLException e){
	    	/** either:
	    	 *    catches that 'etl_flag' column already exists -- a known error. we can ignore & recover. 
  			 *	or:
  			 *	  unknown error. bad. must abort.
	    	 *   
	    	 **/
	    	String infoMsg = "Safe to ignore, this is expected:" + e.getSQLState() + ", " + e.getMessage();
	    	String fatalMsg = "Uh-oh, something bad happened trying to build the ETL_CLEANSING_TABLE. Starting error cleanup.";

	    	sqlStateHandler(c, socj, save1, createCleanseSavepoint, e, infoMsg, fatalMsg);

	    } catch (SQLException e){ //TODO MS Access specific
	    	if ("S0021".equals(e.getSQLState())){
	    		log.debug("ETL_LOGGER:" + e.getSQLState() + ", " + e.getMessage()); //TODO: LOGGING
	    	} else {
	    		errorCleanup(socj, save1, c, null, socj.getFaileddir_csvfiles(), e);
	    		throw SagesOpenCsvJar.abort("Uh-oh, something happened trying to add column etl_flag to ETL_CLEANSING_TABLE.", e); 
	    	}
	    } catch (Exception e) {
	    	errorCleanup(socj, save1, c, null,  socj.getFaileddir_csvfiles(), e);
	    	throw SagesOpenCsvJar.abort("Uh-oh, something happened trying to add column etl_flag to ETL_CLEANSING_TABLE.", e); 
	    }
	}

	/**
	 * SQL state handler examines the state code of sql exception and will
	 * either:
	 *    catches a known error. we can ignore & recover. 
	 *	or:
	 *	  unknown error. bad. must abort.
	 *   
	 * @param c - {@link Connection}
	 * @param save1 - {@link Savepoint}
	 * @param lastSavepoint - {@link Savepoint} the last savepoint corresponding to valid database integrity
	 * @param e - {@link SQLException}
	 * @throws SQLException
	 */
	protected void sqlStateHandler(Connection c, SagesOpenCsvJar socj, Savepoint save1,
			Savepoint lastSavepoint, /*PSQLException e*/ SQLException e, String logMsg, String fatalMsg)
			throws SQLException {
		if (isSqlStateIgnorable(e)){
			log.info(logMsg);
			c.rollback(lastSavepoint);	    		
		} else {
			log.fatal(fatalMsg, e); 
			errorCleanup(socj, save1, c, socj.getCurrentFile(), socj.getFaileddir_csvfiles(), e);
		}
	}
	

	/**
	 * @param c - {@link Connection}
	 * @param socj - {@link SagesOpenCsvJar}
	 * @param save1 - {@link Savepoint}
	 * @param createStagingSavepoint - {@link Savepoint}
	 * @throws SQLException
	 * @throws SagesEtlException
	 */
	public void sqlExceptionHandlerBuildStagingTable(Connection c, 
			SagesOpenCsvJar socj, Savepoint save1,
			Savepoint createStagingSavepoint,
			Exception ex) throws SQLException,
			SagesEtlException {
		try {
			throw ex;
		} catch (PSQLException e){ //TODO: make this generic for SQLException
			String infoMsg = "Safe to ignore, this is expected:" + e.getSQLState() + ", " + e.getMessage();
			String fatalMsg = "Uh-oh, something bad happened trying to build the ETL_STAGING_DB. Starting error cleanup."; 
			sqlStateHandler(c, socj, save1, createStagingSavepoint, e, infoMsg, fatalMsg);

			//	    	if (isSqlStateIgnorable(e)){
				//	    		/** known error. we can ignore & recover. **/
				//	    		log.info("Safe to ignore, this is expected:" + e.getSQLState() + ", " + e.getMessage());
				//	    		c.rollback(createStagingSavepoint);	    		
				//	    	} else {
				//	    		/** unknown error. bad. must abort. **/
				//	    		log.fatal("Uh-oh, something bad happened trying to build the ETL_CLEANSING_TABLE. Starting error cleanup.", e); 
				//	    		errorCleanup(m_socj, save1, c, m_socj.currentFile, socj.faileddir_csvfiles, e);
				//	    	}
			
	    } catch (SQLException e){ //TODO: make this generic for SQLException this is MS Access error
	    	if ("S0001".equals(e.getSQLState())){
	    		log.debug("ETL_LOGGER:" + e.getSQLState() + ", " + e.getMessage()); //TODO: LOGGING
	    	} else {
	    		errorCleanup(socj, save1, c, socj.getCurrentFile(), socj.getFaileddir_csvfiles(), e);
	    		throw SagesOpenCsvJar.abort("Uh-oh, something happened trying to build the ETL_STAGING_DB.", e); 
	    	}
	    } catch (Exception e) {
	    	errorCleanup(socj, save1, c, socj.getCurrentFile(), socj.getFaileddir_csvfiles(), e);
	    	throw SagesOpenCsvJar.abort("Uh-oh, something happened trying to build the ETL_STAGING_DB.", e); 
	    }
	}
	

	/**
	 * @param c - {@link Connection}
	 * @param save1 - {@link Savepoint}
	 * @param createCleanseSavepoint - {@link Savepoint}
	 * @throws SQLException
	 * @throws SagesEtlException
	 */
	public void sqlExceptionHandlerAlterStagingTableAddFlagColumn(
			Connection c, SagesOpenCsvJar socj, Savepoint save1, Savepoint createCleanseSavepoint,
			Exception ex) throws SQLException,
			SagesEtlException {
		
		try {
			throw ex;
		} catch (PSQLException e){
	    	String infoMsg = "Safe to ignore, this is expected:" + e.getSQLState() + ", " + e.getMessage();
	    	String fatalMsg = "Uh-oh, something happened trying to add column etl_flag to ETL_STAGING_DB. Starting error cleanup.";
	    	sqlStateHandler(c, socj, save1, createCleanseSavepoint, e, infoMsg, fatalMsg);
	    } catch (SQLException e){ //TODO MS Access specific
	    	if ("S0021".equals(e.getSQLState())){
	    		log.debug("ETL_LOGGER:" + e.getSQLState() + ", " + e.getMessage()); //TODO: LOGGING
	    	} else {
	    		errorCleanup(socj, save1, c, null, socj.getFaileddir_csvfiles(), e);
	    		throw SagesOpenCsvJar.abort("Uh-oh, something happened trying to add column etl_flag to ETL_CLEANSING_TABLE.", e); 
	    	}
	    } catch (Exception e) {
	    	errorCleanup(socj, save1, c, null, socj.getFaileddir_csvfiles(), e);
	    	throw SagesOpenCsvJar.abort("Uh-oh, something happened trying to add column etl_flag to ETL_STAGING_DB.", e); 
	    }
	}
	
	/**
	 * @param socj_dumb - {@link DumbTestOpenCsvJar}
	 * @param c - {@link Connection}
	 * @param baseLine - {@link Savepoint}
	 * @throws SQLException 
	 * @throws SagesEtlException
	 */
	public void sqlExceptionHandlerTruncateCleanseAndStagingTables(
			DumbTestOpenCsvJar socj_dumb, Connection c, Savepoint baseLine, Exception ex)
			throws SQLException, SagesEtlException {

		try {
			throw ex;
		} catch (SQLException e) {
			String infoMsg = "Safe to ignore, this is expected:" + e.getSQLState() + ", " + e.getMessage();
			String fatalMsg = "Error truncating or executing the etl cleanse -or- staging table: " + e.getMessage();
	    	sqlStateHandler(c, socj_dumb, baseLine, baseLine, e, infoMsg, fatalMsg);
					//			if (isSqlStateIgnorable(e)){
					//	    		/** known error. we can ignore & recover. **/
					//	    		log.info("Safe to ignore, this is expected:" + e.getSQLState() + ", " + e.getMessage());
					//	    		c.rollback(baseLine);
					//	    	} else {
					//	    		/** unknown error. bad. must abort. **/
					//	    		log.fatal("Error truncating or executing the etl cleanse -or- staging table: " + e.getMessage());
					//	    		errorCleanup(socj_dumb, baseLine, c, null, socj_dumb.faileddir_csvfiles, e);
					//	    		throw SagesOpenCsvJar.abort(e.getMessage(), e);
					//	    	}
		} catch (Exception e) {
			// TODO - is this really needed. investigate later...
    		/** unknown error. bad. must abort. **/
    		log.fatal("Error truncating or executing the etl cleanse -or- staging table: " + e.getMessage());
    		errorCleanup(socj_dumb, baseLine, c, null, socj_dumb.getFaileddir_csvfiles(), e);
    		throw SagesOpenCsvJar.abort(e.getMessage(), e);
	    }
	}
	
	
	/**
	 * error handling code. transactions rollbacks, file moving/deletion
	 * 
	 * @param socj {@link SagesOpenCsvJar} the strategy belongs to
	 * @param savepoint {@link Savepoint} to rollback to
	 * @param connection {@link Connection} database connection
	 * @param currentCsv {@link File} file currently being processed by ETL
	 * @param failedDirPath {@link String} file directory to move failed file
	 * @param e {@link Exception} exception that caused the error. used in logging output
	 * @return
	 */
	public int errorCleanup(SagesOpenCsvJar socj, Savepoint savepoint, Connection connection, File currentCsv, String failedDirPath, Exception e){

		String savepointName = "";
	    socj.setSuccess(false);
	    File fileRefForStatusLogging = null;
	    
	    int errorFlag = 0;
			try {
				log.error("ERROR CLEANUP DUE TO EXCEPTION:\n" + e.getMessage());
				e.printStackTrace();
				savepointName = savepoint.getSavepointName();
				connection.rollback(savepoint);
				connection.commit();
				/** 
				 * MOVE CURRENT CSV OVER TO FAILED, 
				 * TODO: WRITE TO LOG: FILE_X FAILED, FAILURE OCCURED AT STEP_X **/
				if (currentCsv != null) {
					socj.getFailedCsvFiles().add(currentCsv);
			    	Date date = new Date();
			    	long dtime = date.getTime();
			    	
			    	/** failure destination dir **/
			    	File dir = new File(failedDirPath);

			    	/** Move file to new dir **/
			    	fileRefForStatusLogging = new File(dir, dtime + "_"+ currentCsv.getName());
			    	FileUtils.copyFile(currentCsv, fileRefForStatusLogging);
			    	FileUtils.forceDelete(currentCsv);
					SagesOpenCsvJar.logFileOutcome(socj, connection, fileRefForStatusLogging, "FAILURE");

				}
			} catch(IOException io){
				log.error(io.getMessage());
			} catch (SQLException e1) {
				log.error(e1.getMessage());
			} catch (SagesEtlException e2) {
				log.error(e2.getMessage());
			} finally {
				log.error("SYSTEM ROLLED BACK TO SAVEPOINT = " + savepointName);
				/** This is an error occurring with creating the stage and cleanse table. no recovery option. **/
				if ("save1".equals(savepointName)){
					errorFlag = 1;
					log.fatal("This is an error occurring with creating the stage and cleanse table. no recovery option.");
					System.exit(-1);
				} 
				/** This is an error occurring with entering data. we attempt with next file **/
				if ("save2".equals(savepointName)){
					errorFlag = 2;
				} 
		}
		 return errorFlag;	
		}
}
