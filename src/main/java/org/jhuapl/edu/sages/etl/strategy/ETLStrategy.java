/**
 * 
 */
package org.jhuapl.edu.sages.etl.strategy;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.ArrayList;

import org.jhuapl.edu.sages.etl.SagesEtlException;
import org.jhuapl.edu.sages.etl.oldstuff.TestOpenCsvJar;
import org.jhuapl.edu.sages.etl.opencsvpods.DumbTestOpenCsvJar;

/**
 * Design Pattern: Strategy
 * {@link ETLStrategy} defines the various methods that the SAGES ETL process will need to fulfill. The 
 * actual implementation can vary depending on the target database 
 * (i.e. Postgresql supports Savepoints, MSAccess does not; i.e. Variations in SQL syntax)
 * 
 * 
 * @author POKUAM1
 * @created Nov 1, 2011
 */
public interface ETLStrategy {

	/** extract headers from CSV file **/
	void extractHeaderColumns(SagesOpenCsvJar socj) throws FileNotFoundException, IOException;
	/** determining the headers from CSV file **/
	String[] determineHeaderColumns(File file) throws FileNotFoundException, IOException;
	
	/** Adds ETL_FLAG column to a table **/
	String addFlagColumn(String tableToModify);
	
	/** truncate cleansing & staging tables 
	 * @throws SagesEtlException 
	 * @throws SQLException **/
	void truncateCleanseAndStagingTables(DumbTestOpenCsvJar socj_dumb, Connection c, File file,
			Savepoint groundZero) throws SagesEtlException, SQLException;
	/** creating cleanse table **/
	Savepoint buildCleanseTable(Connection c, SagesOpenCsvJar socj, Savepoint save1) throws SQLException,SagesEtlException;
	/** Adds ETL_FLAG column to cleanse table **/
	void alterCleanseTableAddFlagColumn(Connection c, Savepoint save1, Savepoint createCleanseSavepoint) throws SQLException, SagesEtlException;

	/** create staging table **/
	Savepoint buildStagingTable(Connection c, SagesOpenCsvJar socj, Savepoint save1) throws SQLException, SagesEtlException;
	/** Adds ETL_FLAG column to staging table **/
	void alterStagingTableAddFlagColumn(Connection c, Savepoint save1, Savepoint createCleanseSavepoint) throws SQLException, SagesEtlException;
	
	/** determining source-to-destination column mappings **/
	void generateSourceDestMappings(SagesOpenCsvJar socj);
	
	/** building SQL for inserting into cleansing table **/
	String buildInsertIntoCleansingTableSql(Connection c, SagesOpenCsvJar socj) throws SQLException;
	/** set & execute SQL for inserting into cleansing table **/
	void setAndExecuteInsertIntoCleansingTablePreparedStatement(Connection c, SagesOpenCsvJar socj, 
			ArrayList<String[]> entries_rawdata, Savepoint save2,
			PreparedStatement ps_INSERT_CLEANSE) throws SQLException;

	/**
	 * building & executing SQL for copying from cleansing to staging table
	 * @param c
	 * @param socj
	 * @param save2
	 * @throws SQLException
	 * @throws SagesEtlException
	 */
	void copyFromCleanseToStaging(Connection c, SagesOpenCsvJar socj, Savepoint save2) throws SQLException, SagesEtlException;

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
	int errorCleanup(SagesOpenCsvJar socj, Savepoint savepoint, Connection connection, File currentCsv, String failedDirPath, Exception e);
	
}
