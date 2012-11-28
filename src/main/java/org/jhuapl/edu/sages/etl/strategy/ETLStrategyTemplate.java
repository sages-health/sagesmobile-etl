/**
 * 
 */
package org.jhuapl.edu.sages.etl.strategy;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.jhuapl.edu.sages.etl.ETLProperties;
import org.jhuapl.edu.sages.etl.SagesEtlException;
import org.jhuapl.edu.sages.etl.SqlStateHandler;
import org.jhuapl.edu.sages.etl.opencsvpods.DumbTestOpenCsvJar;
import org.postgresql.util.PSQLException;

import au.com.bytecode.opencsv.CSVReader;

/**
 * Design Pattern: Strategy
 * {@link ETLStrategyTemplate} defines the various methods that the SAGES ETL process will need to fulfill. The 
 * actual implementation can vary depending on the target database 
 * (i.e. Postgresql supports Savepoints, MSAccess does not; i.e. Variations in SQL syntax)
 * 
 * 
 * @author POKUAM1
 * @created Nov 1, 2011
 */
public abstract class ETLStrategyTemplate {

	private static final Logger log = Logger.getLogger(ETLStrategyTemplate.class);
	
	protected SagesOpenCsvJar m_socj;
	protected SqlStateHandler m_sqlStateHandler;
	
	static protected Set<String> ignorableErrorCodes = new HashSet<String>(){{
		add("CODE");
	}};
	
	/**
	 * 
	 */
	public ETLStrategyTemplate() {
		m_sqlStateHandler = new SqlStateHandler();
		m_sqlStateHandler.setIgnorableErrorCodes(ignorableErrorCodes);
	}
//	/**
//	 * @param e {@link SQLException}
//	 * @return boolean if sql state code is in the ignorable set of codes
//	 */
//	protected boolean isSqlStateIgnorable(SQLException e) {
//		return ignorableErrorCodes.contains(e.getSQLState());
//	}
	
	/** extract headers from CSV file **/
	public void extractHeaderColumns(SagesOpenCsvJar socj) throws FileNotFoundException, IOException {
		/** header columns used to define the CLEANSE table schema */
		socj.header_src = new String[0];
		
		/** determine header columns **/
		File csvinputDir = new File(socj.inputdir_csvfiles);
		if (csvinputDir.isDirectory()){
			socj.csvFiles = csvinputDir.listFiles();
			if (socj.csvFiles.length == 0){
				log.info("Input directory has no files. Nothing to load into database. Exiting.");
				log.info("Your input directory is located here: " + socj.inputdir_csvfiles);
				System.exit(0); 
			} 
			socj.header_src = determineHeaderColumns(socj.csvFiles[0]);
		} else {
			log.fatal(csvinputDir.getName() + "is not valid csv input directory. Exiting.");
			System.exit(-1); 	
		}
	}
	
	/** determining the headers from CSV file 
	 * @param file 
	 * @return headers
	 **/
	public String[] determineHeaderColumns(File file)throws FileNotFoundException, IOException {
		
		CSVReader reader_rawdata = new CSVReader(new FileReader(file));
		ArrayList<String[]> currentEntries = (ArrayList<String[]>) reader_rawdata.readAll();
		String[] headerColumns = currentEntries.get(0); /** set header from the first csv file */
		reader_rawdata.close();
		//currentEntries.remove(0); /** remove the header row */
		//master_entries_rawdata.addAll(currentEntries);
		return headerColumns;
	}
	
	/** Adds ETL_FLAG column to a table *
	 * 
	 * @param tableToModify name of table to modify
	 * @return alter table statement 
	 **/
	public String addFlagColumn(String tableToModify) {
		
		String sqlaltertableAddColumn = "ALTER TABLE " + tableToModify + " ADD COLUMN etl_flag  varchar(255)";
		return sqlaltertableAddColumn;
	}
	
	/** truncate cleansing & staging tables 
	 * @param socj_dumb {@link DumbTestOpenCsvJar}
	 * @param c {@link Connection}
	 * @param file csv file being processed
	 * @param baseLine {@link Savepoint} savepoint to rollback to if error occurs
	 * @throws SagesEtlException 
	 * @throws SQLException 
	 ***/
	public void truncateCleanseAndStagingTables(DumbTestOpenCsvJar socj_dumb, Connection c, File file,Savepoint baseLine) 
			throws SagesEtlException, SQLException {
		
		log.info("--TRUNCATE CLEANSE & STAGING TABLES--");
		socj_dumb.src_table_name = socj_dumb.props_etlconfig.getProperty("dbprefix_src") + "_" + SagesOpenCsvJar.ETL_CLEANSE_TABLE;
		socj_dumb.dst_table_name = socj_dumb.props_etlconfig.getProperty("dbprefix_dst") + "_" + SagesOpenCsvJar.ETL_STAGING_DB;

		try {
			PreparedStatement ps_TRUNCATECleanseTable = c.prepareStatement("TRUNCATE TABLE " + socj_dumb.src_table_name);
			PreparedStatement ps_TRUNCATEStagingTable = c.prepareStatement("TRUNCATE TABLE " + socj_dumb.dst_table_name);

			ps_TRUNCATECleanseTable.execute();
			ps_TRUNCATEStagingTable.execute();
			c.commit();
		} catch (Exception e) {
			m_sqlStateHandler.sqlExceptionHandlerTruncateCleanseAndStagingTables(socj_dumb, c, baseLine, e);
		}
	}


	/** creating etl status table **/ 
	/************************************
	 * build ETL_STATUS_TABLE 
	 * **********************************
	 * SQL: "CREATE TABLE..."
	 * 
	 ************************************/
	public Savepoint buildEtlStatusTable(Connection c, SagesOpenCsvJar socj, Savepoint save1) throws SagesEtlException, SQLException {
		Savepoint createEtlStatusSavepoint = c.setSavepoint("createEtlStatusSavepoint");
		
		// CREATE etl_status table
		String createSql = "CREATE TABLE etl_status ("
				+ "filename character varying(500),"
				+ "filepath character varying(500),"
				+ "outcome character varying(255),"
				+ "processtime timestamp without time zone)";
		
		try {
			PreparedStatement ps_CREATEFILESTATUS = c.prepareStatement(createSql);
			ps_CREATEFILESTATUS.execute();
			c.commit();
		} catch (Exception e) {
			m_sqlStateHandler.sqlExceptionHandlerBuildEtlStatusTable(c, socj, save1, createEtlStatusSavepoint, e);
		} 
		return createEtlStatusSavepoint;
	}
	
	/** creating cleanse table **/
	/*********************************** 
     * build ETL_CLEANSE_TABLE 
     ***********************************
     * SQL: "CREATE TABLE..." 
     * - all columns have text sql-datatype
     * - column definitions built from csv file header 
     ****************************/
	public Savepoint buildCleanseTable(Connection c, SagesOpenCsvJar socj, Savepoint save1) throws SagesEtlException, SQLException {
		
	     //http://postgresql.1045698.n5.nabble.com/25P02-current-transaction-is-aborted-commands-ignored-until-end-of-transaction-block-td2174290.html
		socj.src_table_name = socj.props_etlconfig.getProperty("dbprefix_src") + "_" + SagesOpenCsvJar.ETL_CLEANSE_TABLE;
	    String createStmt_src = "CREATE TABLE " + socj.src_table_name + " \n(\n";
	    
	    /**
	     * build schema definition & map column to its index for later use with ResultSet and PreparedStatment
	     */
	    int zsrc=1;
	    for (String colHead_src: socj.header_src){
	    	if (socj.dbms.equals(ETLProperties.dbid_msaccess) && "time".equals(colHead_src)){
	    		colHead_src = "accessetl_time";
	    	}
	    	createStmt_src += colHead_src + " varchar(255),\n";
	    	socj.PARAMINDX_SRC.put(colHead_src, zsrc);
	    	zsrc++;
	    }
	   
	    /**remove trailing ',' **/
	    createStmt_src = StringUtils.substringBeforeLast(createStmt_src, ",\n") + "\n);";
	    
	    log.info("\ncreatestmt:\n" + createStmt_src);
	    PreparedStatement PS_create_CLEANSE = c.prepareStatement(createStmt_src);
	    
	    Savepoint createCleanseSavepoint = c.setSavepoint("createCleanseSavepoint");
	    try {
	    	PS_create_CLEANSE.execute(); //TODO: pgadmin wanted a pk for me to edit thru gui
	    } catch (Exception e){
	    	m_sqlStateHandler.sqlExceptionHandlerBuildCleanseTable(c, socj, save1, createCleanseSavepoint, e);
	    	
	    }
	    
		return createCleanseSavepoint;
	}

	/** Adds ETL_FLAG column to cleanse table **/
	public void alterCleanseTableAddFlagColumn(Connection c, Savepoint save1, Savepoint createCleanseSavepoint)
			throws SQLException, SagesEtlException {
		
		String sqlaltertableAddColumn = addFlagColumn(m_socj.src_table_name);
	    PreparedStatement PS_addcolumn_Flag = c.prepareStatement(sqlaltertableAddColumn);
	    log.info("ALTER STATEMENT: " + sqlaltertableAddColumn);

	    try {
	    	PS_addcolumn_Flag.execute();
	    } catch (Exception e) {
	    	m_sqlStateHandler.sqlExceptionHandlerAlterCleanseTableAddFlagColumn(c, m_socj, save1, createCleanseSavepoint, e);
	    }
	}

	/** create staging table **/
	abstract Savepoint buildStagingTable(Connection c, SagesOpenCsvJar socj, Savepoint save1) throws SQLException, SagesEtlException;
	
	/** Adds ETL_FLAG column to existing staging table 
	 * 
	 * @param c - {@link Connection}
	 * @param save1 - {@link Savepoint}
	 * @param createCleanseSavepoint - {@link Savepoint}
	 * 
	 ***/
	public void alterStagingTableAddFlagColumn(Connection c, Savepoint save1,
			Savepoint createCleanseSavepoint) throws SQLException,
			SagesEtlException {

		String sqlaltertableAddColumn;
		PreparedStatement PS_addcolumn_Flag;
		sqlaltertableAddColumn = addFlagColumn(m_socj.dst_table_name);
	    PS_addcolumn_Flag = c.prepareStatement(sqlaltertableAddColumn);
	    log.debug("ALTER STATEMENT: " + sqlaltertableAddColumn);
	    
		try {
	    	PS_addcolumn_Flag.execute();
	    } catch (Exception e){
	    	m_sqlStateHandler.sqlExceptionHandlerAlterStagingTableAddFlagColumn(c, m_socj, save1, createCleanseSavepoint, e);
	    }
	}	
	/** determining source-to-destination column mappings 
	 * @param socj {@link SagesOpenCsvJar}
	 **/
	public void generateSourceDestMappings(SagesOpenCsvJar socj) {
		
		socj.MAPPING_MAP = new LinkedHashMap<String,String>();
		socj.MAPPING_REV_MAP = new LinkedHashMap<String,String>();
		
		for (Entry<Object,Object> e : socj.props_mappings.entrySet()){
			String key = ((String)e.getKey()).trim();
			String value = ((String)e.getValue()).trim();
			socj.MAPPING_MAP.put(key, value);
			socj.MAPPING_REV_MAP.put(value, key);
		}		
	}
	
	/** building SQL for inserting into cleansing table **/
	abstract String buildInsertIntoCleansingTableSql(Connection c, SagesOpenCsvJar socj) throws SQLException;
	
	/** set & execute SQL for inserting into cleansing table **/
	abstract void setAndExecuteInsertIntoCleansingTablePreparedStatement(Connection c, SagesOpenCsvJar socj, 
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
	abstract void copyFromCleanseToStaging(Connection c, SagesOpenCsvJar socj, Savepoint save2) throws SQLException, SagesEtlException;

	public int errorCleanup(SagesOpenCsvJar socj, Savepoint savepoint, Connection connection, File currentCsv, String failedDirPath, Exception e){
		return m_sqlStateHandler.errorCleanup(socj, savepoint, connection, currentCsv, failedDirPath, e);
	}

	
}
