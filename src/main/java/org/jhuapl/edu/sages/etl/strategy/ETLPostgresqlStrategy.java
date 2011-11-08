/**
 * 
 */
package org.jhuapl.edu.sages.etl.strategy;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Types;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.jhuapl.edu.sages.etl.ETLProperties;
import org.jhuapl.edu.sages.etl.SagesEtlException;
import org.jhuapl.edu.sages.etl.opencsvpods.DumbTestOpenCsvJar;
import org.postgresql.util.PSQLException;

import au.com.bytecode.opencsv.CSVReader;

/**
 * {@link ETLPostgresqlStrategy} is the Postgresql specific strategy for the ETL processing logic. 
 * Transaction handling, SQL syntax nuances, and error codes are for Postgresql.
 * @author POKUAM1
 * @created Nov 1, 2011
 */
public class ETLPostgresqlStrategy implements ETLStrategy {
	private static final Logger log = Logger.getLogger(ETLPostgresqlStrategy.class);
	public static final Map<String, String> errorcodes = new HashMap<String, String>(){{

		put("CODE", "CODE");
	}};
	
	public static final Set<String> ignorableErrorCodes = new HashSet<String>(){{
		
		// "42P07": relation "oevisit_etl_cleanse_table" already exists
		// "42P07": relation "oevisit_etl_staging_db" already exists
		add("42P07");
		
		//add("25P02"); // column "etl_flag" of relation "oevisit_etl_staging_db" already exists
		
		// "42701": column "etl_flag" of relation "oevisit_etl_cleanse_table" already exists
		// "42701": column "etl_flag" of relation "oevisit_etl_staging_db" already exists
		add("42701"); 

		// "42P01": relation "oevisit_etl_cleanse_table" does not exist
		// "42P01": relation "oevisit_etl_staging_db" does not exist
		add("42P01");
		add("CODE");
		add("CODE");
		add("CODE");
	}};
	private SagesOpenCsvJar m_socj;
	
	public ETLPostgresqlStrategy(SagesOpenCsvJar socj){
		m_socj = socj;
	}

	public ETLPostgresqlStrategy(){
		
	}
	
	/* (non-Javadoc)
	 * @see org.jhuapl.edu.sages.etl.ETLStrategy#determineHeaderColumns(java.io.File)
	 */
	@Override
	public String[] determineHeaderColumns(File file)throws FileNotFoundException, IOException {
		CSVReader reader_rawdata = new CSVReader(new FileReader(file));
		ArrayList<String[]> currentEntries = (ArrayList<String[]>) reader_rawdata.readAll();
		String[] headerColumns = currentEntries.get(0); /** set header from the first csv file */
		reader_rawdata.close();
		//currentEntries.remove(0); /** remove the header row */
		//master_entries_rawdata.addAll(currentEntries);
		return headerColumns;
	}

	
	/* (non-Javadoc)
	 * @see org.jhuapl.edu.sages.etl.ETLStrategy#addFlagColumn(java.lang.String)
	 */
	@Override
	public String addFlagColumn(String tableToModify) {
		String sqlaltertableAddColumn = "ALTER TABLE " + tableToModify + " ADD COLUMN etl_flag  varchar(255)";
		return sqlaltertableAddColumn;
	}

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
	    	if (ignorableErrorCodes.contains(e.getSQLState())){
	    		/** catches that 'etl_flag' column already exists **/
	    		/** known error. we can ignore & recover. **/
	    		log.info("Safe to ignore, this is expected:" + e.getSQLState() + ", " + e.getMessage());
	    		c.rollback(createCleanseSavepoint);	    		
	    	} else {
	    		/** unknown error. bad. must abort. **/
	    		log.fatal("Uh-oh, something bad happened trying to build the ETL_CLEANSING_TABLE. Starting error cleanup.", e); 
	    		errorCleanup(m_socj, save1, c, m_socj.currentFile, m_socj.faileddir_csvfiles, e);
	    	}

	    } catch (SQLException e){ //TODO MS Access specific
	    	if ("S0021".equals(e.getSQLState())){
	    		log.debug("ETL_LOGGER:" + e.getSQLState() + ", " + e.getMessage()); //TODO: LOGGING
	    	} else {
	    		errorCleanup(m_socj, save1, c, null, m_socj.faileddir_csvfiles, e);
	    		throw SagesOpenCsvJar.abort("Uh-oh, something happened trying to add column etl_flag to ETL_CLEANSING_TABLE.", e); 
	    	}
	    } catch (Exception e) {
	    	errorCleanup(m_socj, save1, c, null,  m_socj.faileddir_csvfiles, e);
	    	throw SagesOpenCsvJar.abort("Uh-oh, something happened trying to add column etl_flag to ETL_CLEANSING_TABLE.", e); 
	    }
	}

	
	/* (non-Javadoc)
	 * @see org.jhuapl.edu.sages.etl.ETLStrategy#alterStagingTableAddFlagColumn(java.sql.Connection, java.sql.Savepoint, java.sql.Savepoint)
	 */
	@Override
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
	    } catch (PSQLException e){
	    	if (ignorableErrorCodes.contains(e.getSQLState())){
	    		/** catches that 'etl_flag' column already exists. known error we can ignore & recover. **/
	    		log.info("Safe to ignore, this is expected:" + e.getSQLState() + ", " + e.getMessage());
	    		c.rollback(createCleanseSavepoint);	    		
	    	} else {
	    		/** unknown error. bad. must abort. **/
	    		log.fatal("Uh-oh, something bad happened trying to build the ETL_CLEANSING_TABLE. Starting error cleanup.", e); 
	    		errorCleanup(m_socj, save1, c, m_socj.currentFile, m_socj.faileddir_csvfiles, e);
	    	}
	    } catch (SQLException e){ //TODO MS Access specific
	    	if ("S0021".equals(e.getSQLState())){
	    		log.debug("ETL_LOGGER:" + e.getSQLState() + ", " + e.getMessage()); //TODO: LOGGING
	    	} else {
	    		errorCleanup(m_socj, save1, c, null, m_socj.faileddir_csvfiles, e);
	    		throw SagesOpenCsvJar.abort("Uh-oh, something happened trying to add column etl_flag to ETL_CLEANSING_TABLE.", e); 
	    	}
	    } catch (Exception e) {
	    	errorCleanup(m_socj, save1, c, null, m_socj.faileddir_csvfiles, e);
	    	throw SagesOpenCsvJar.abort("Uh-oh, something happened trying to add column etl_flag to ETL_STAGING_DB.", e); 
	    }
	}

	/* (non-Javadoc)
	 * @see org.jhuapl.edu.sages.etl.strategy.ETLStrategy#extractHeaderColumns(org.jhuapl.edu.sages.etl.opencsvpods.SagesOpenCsvJar)
	 */
	@Override
	public void extractHeaderColumns(SagesOpenCsvJar socj) throws FileNotFoundException, IOException {
		/** header columns used to define the CLEANSE table schema */
		socj.header_src = new String[0];
		
		/** determine header columns **/
		File csvinputDir = new File(socj.inputdir_csvfiles);
		if (csvinputDir.isDirectory()){
			socj.csvFiles = csvinputDir.listFiles();
			if (socj.csvFiles.length == 0){
				log.info("Input directory has no files. Nothing to load into database. Exiting.");
				System.exit(0); 
			} 
			socj.header_src = determineHeaderColumns(socj.csvFiles[0]);
		} else {
			log.fatal(csvinputDir.getName() + "is not valid csv input directory. Exiting.");
			System.exit(-1); 	
		}
	}
	/**
	 * @param socj_dumb
	 * @param c
	 * @param file
	 * @param groundZero
	 * @throws SagesEtlException
	 * @throws SQLException 
	 */
	public void truncateCleanseAndStagingTables(DumbTestOpenCsvJar socj_dumb, Connection c, File file,
			Savepoint groundZero) throws SagesEtlException, SQLException {
		log.info("--TRUNCATE CLEANSE & STAGING--");
		socj_dumb.src_table_name = socj_dumb.props_etlconfig.getProperty("dbprefix_src") + "_" + SagesOpenCsvJar.ETL_CLEANSE_TABLE;
		socj_dumb.dst_table_name = socj_dumb.props_etlconfig.getProperty("dbprefix_dst") + "_" + SagesOpenCsvJar.ETL_STAGING_DB;

		try {
			PreparedStatement ps_TRUNCATECleanseTable = c.prepareStatement("TRUNCATE TABLE " + socj_dumb.src_table_name);
			PreparedStatement ps_TRUNCATEStagingTable = c.prepareStatement("TRUNCATE TABLE " + socj_dumb.dst_table_name);

			ps_TRUNCATECleanseTable.execute();
			ps_TRUNCATEStagingTable.execute();
			c.commit();
		} catch (SQLException e) {
			if (ignorableErrorCodes.contains(e.getSQLState())){
	    		/** known error. we can ignore & recover. **/
	    		log.info("Safe to ignore, this is expected:" + e.getSQLState() + ", " + e.getMessage());
	    		c.rollback(groundZero);
	    	} else {
	    		/** unknown error. bad. must abort. **/
	    		log.fatal("Error truncating or executing the etl cleanse -or- staging table: " + e.getMessage());
	    		errorCleanup(socj_dumb, groundZero, c, null, socj_dumb.faileddir_csvfiles, e);
	    		throw SagesOpenCsvJar.abort(e.getMessage(), e);
	    	}
		}
	}
	
	
	/* (non-Javadoc)
	 * @see org.jhuapl.edu.sages.etl.strategy.ETLStrategy#buildCleanseTable(java.sql.Connection, org.jhuapl.edu.sages.etl.opencsvpods.SagesOpenCsvJar, java.sql.Savepoint)
	 */
	@Override
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
	    } catch (PSQLException e){ //TODO: make this generic for SQLException
	    	if (ignorableErrorCodes.contains(e.getSQLState())){
	    		/** known error. we can ignore & recover. **/
	    		log.info("Safe to ignore, this is expected:" + e.getSQLState() + ", " + e.getMessage()); //TODO: LOGGING
	    		c.rollback(createCleanseSavepoint);
	    		
	    	} else {
	    		/** unknown error. bad. must abort. **/
	    		log.fatal("Uh-oh, something bad happened trying to build the ETL_CLEANSING_TABLE. Starting error cleanup.", e); 
	    		errorCleanup(m_socj, save1, c, m_socj.currentFile, socj.faileddir_csvfiles, e);
	    	}
	    } catch (SQLException e){ //TODO: make this generic for SQLException this is MS Access error
	    	if ("S0001".equals(e.getSQLState())){
	    		log.debug("ETL_LOGGER:" + e.getSQLState() + ", " + e.getMessage()); //TODO: LOGGING
	    	} else {
	    		errorCleanup(m_socj, save1, c, m_socj.currentFile, socj.faileddir_csvfiles, e);
	    		throw SagesOpenCsvJar.abort("Uh-oh, something happened trying to build the ETL_CLEANSING_TABLE.", e); 
	    	}
	    } catch (Exception e) {
	    	errorCleanup(m_socj, save1, c, m_socj.currentFile, socj.faileddir_csvfiles, e);
	    	throw SagesOpenCsvJar.abort("Uh-oh, something happened trying to build the ETL_CLEANSING_TABLE.", e); 
	    }
		return createCleanseSavepoint;
	}
	
	/* (non-Javadoc)
	 * @see org.jhuapl.edu.sages.etl.strategy.ETLStrategy#buildStagingTable(java.sql.Connection, org.jhuapl.edu.sages.etl.opencsvpods.SagesOpenCsvJar, java.sql.Savepoint)
	 */
	@Override
	public Savepoint buildStagingTable(Connection c, SagesOpenCsvJar socj,
			Savepoint save1) throws SQLException, SagesEtlException {

		/***************************************************************************
	     * build ETL_STAGING_TABLE 
	     ***************************************************************************
	     * SQL: "CREATE TABLE..." 
	     * - all columns have sql-datatype identical to FINAL DESTINATION TABLE
	     * - column definitions build from metadata of FINAL DESTINATION TABLE
	     ***************************************************************************/
    
	    /** get metadata for FINAL DESTINATION TABLE and use it to build STAGING */
		DatabaseMetaData dbmd = c.getMetaData();
		String catalog = null;
		String schemaPattern = null;
		//String tableNamePattern = "etl_individual";
		String tableNamePattern = socj.props_etlconfig.getProperty("productionTableName");
		String columnNamePattern = null;
		
		ResultSet rs_FINAL = dbmd.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);

		String destTableStr = "";
		socj.dst_table_name = socj.props_etlconfig.getProperty("dbprefix_dst") + "_" + SagesOpenCsvJar.ETL_STAGING_DB;
		socj.DEST_COLTYPE_MAP = new LinkedHashMap<String, String>();
		socj.DEST_SQLTYPE_MAP = new LinkedHashMap<String, Integer>();
		int zdst=1;
		
		/** 
		 * figure out the columns in the target destination table, auto-inc columns are skipped 
		 * additionally populate these maps:
		 * 	DEST_COLTYPE_MAP[colname:coltype] 
		 * 	DEST_SQLTYPE_MAP[colname:colSQLtype]
		 *  PARAMINDX_DST[colname:sqlparamindex]
		 */
		while (rs_FINAL.next()){
			/**http://stackoverflow.com/questions/1870022/java-resultset-hasnext
			//http://www.herongyang.com/JDBC/sqljdbc-jar-Column-List.html */
			//log.debug("column check");
			String colName = rs_FINAL.getString("COLUMN_NAME");
			String colType = rs_FINAL.getString("TYPE_NAME");
			int colSqlType = rs_FINAL.getInt("DATA_TYPE");
			String isAutoInc = "";
			try {
				isAutoInc = rs_FINAL.getString("IS_AUTOINCREMENT");  /**YES, NO, or "" */
			} catch (SQLException e) {
				if ("S0022".equals(e.getSQLState())){ // MS Access specific
					log.debug("ETL_LOGGER:" + "this database does not support IS_AUTOINCREMENT result meta data column. safe to ignore");
				}
			}
			if ("serial".equalsIgnoreCase(colType) || "COUNTER".equalsIgnoreCase(colType)|| "YES".equalsIgnoreCase(isAutoInc)) {
				continue;
			}
			
			String colDef = colName + " " + colType + ",";
			destTableStr += colDef + "\n";

			log.debug(colName + "=" + colType + "("+ colSqlType + ")"); //TODO: LOGGING
			/** TODO: make sure using this staging table map <colName:colType>  **/
			socj.DEST_COLTYPE_MAP.put(colName, colType);
			socj.DEST_SQLTYPE_MAP.put(colName, colSqlType);
			socj.PARAMINDX_DST.put(colName, zdst);
	    	zdst++;
		}
		
		/** the built "CREATE TABLE STAGING_TABLE..." string */
	    Savepoint createStagingSavepoint = c.setSavepoint("createStagingSavePoint");
		/** remove trailing ','  **/
	    destTableStr = StringUtils.substringBeforeLast(destTableStr, ",");

	    String createStagingStmt = "CREATE TABLE " + socj.dst_table_name + "\n(\n" + destTableStr + "\n);";
		log.info(createStagingStmt);
		
		PreparedStatement ps_CREATE_STAGING = c.prepareStatement(createStagingStmt);
		
		try {
			/** execute CREATE STAGING_TABLE sql */
			ps_CREATE_STAGING.execute();
		} catch (PSQLException e){ //TODO: make this generic for SQLException
	    	if (ignorableErrorCodes.contains(e.getSQLState())){
	    		/** known error. we can ignore & recover. **/
	    		log.info("Safe to ignore, this is expected:" + e.getSQLState() + ", " + e.getMessage());
	    		c.rollback(createStagingSavepoint);	    		
	    	} else {
	    		/** unknown error. bad. must abort. **/
	    		log.fatal("Uh-oh, something bad happened trying to build the ETL_CLEANSING_TABLE. Starting error cleanup.", e); 
	    		errorCleanup(m_socj, save1, c, m_socj.currentFile, socj.faileddir_csvfiles, e);
	    	}
	    } catch (SQLException e){ //TODO: make this generic for SQLException this is MS Access error
	    	if ("S0001".equals(e.getSQLState())){
	    		log.debug("ETL_LOGGER:" + e.getSQLState() + ", " + e.getMessage()); //TODO: LOGGING
	    	} else {
	    		errorCleanup(m_socj, save1, c, m_socj.currentFile, socj.faileddir_csvfiles, e);
	    		throw SagesOpenCsvJar.abort("Uh-oh, something happened trying to build the ETL_CLEANSING_TABLE.", e); 
	    	}
	    } catch (Exception e) {
	    	errorCleanup(m_socj, save1, c, m_socj.currentFile, socj.faileddir_csvfiles, e);
	    	throw SagesOpenCsvJar.abort("Uh-oh, something happened trying to build the ETL_STAGING_DB.", e); 
	    }
		return createStagingSavepoint; 
	}

	
	/* (non-Javadoc)
	 * @see org.jhuapl.edu.sages.etl.strategy.ETLStrategy#generateSourceDestMappings(org.jhuapl.edu.sages.etl.opencsvpods.SagesOpenCsvJar)
	 */
	@Override
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
	
	@Override
	public String buildInsertIntoCleansingTableSql(Connection c, SagesOpenCsvJar socj) throws SQLException {
	    /************************************************ 
	     * build reusable 'INSERT INTO CLEANSING_TABLE'
	     ************************************************
	     * SQL: "INSERT INTO SRC_TABLE..."
	     * Example SQL: "INSERT INTO src_table_name VALUES (?, ?, ?, ?, ?,...)"
	     * data will be inserted as text sql-datatype
	     *  
	     */

		int lastTick;
		String insertStmt_src = "INSERT INTO " + socj.src_table_name + " VALUES (";
	    
	    for (int h=0; h < socj.header_src.length; h++){
	    	insertStmt_src = insertStmt_src + "?,"; 
	    }
	    
    	/***
    	 * MS Access specific 
    	 * 2351 - Microsoft Access can't represent an implicit VALUES clause in the query design grid. 
    	 * Edit this in SQL view.
    	 * 
    	 * (this is for the "etl_flag" column that was added after table creation
    	 */
    	if (socj.dbms.equals(ETLProperties.dbid_msaccess)){
    		insertStmt_src = insertStmt_src + "?,"; 
    	}
    	
	    /** remove trailing ','  TODO CLEAN UP WITH StringUtils.join() */
    	
    	insertStmt_src = StringUtils.substringBeforeLast(insertStmt_src, ",") + ");";
    	
    	log.debug("ETL_LOGGER\ninsertstmt_src: " + insertStmt_src); //TODO: LOGGING
    	return insertStmt_src;
	}

	@Override
	public void setAndExecuteInsertIntoCleansingTablePreparedStatement(
			Connection c, SagesOpenCsvJar socj, ArrayList<String[]> entries_rawdata, Savepoint save2,
			PreparedStatement ps_INSERT_CLEANSE) throws SQLException {
		
		/** set values for the ? parameters, NOTE all values have text sql-datatype */
	    for (int e=0; e < entries_rawdata.size(); e++){
	    	String[] entry = entries_rawdata.get(e);
	    	String log_insertStmt = "VALUES:"; //TODO: LOGGING
	    	
	    	for (int p=0; p < entry.length; p++){
	    		ps_INSERT_CLEANSE.setString(p+1, entry[p]);
	    		log_insertStmt += "'" + entry[p] + "',"; 
	    	}
	    	
	    	/***
	    	 * MS Access specific 
	    	 * 2351 - Microsoft Access can't represent an implicit VALUES clause in the query design grid. 
	    	 * Edit this in SQL view.
	    	 * 
	    	 */
	    	if (socj.dbms.equals(ETLProperties.dbid_msaccess)){
	    		ps_INSERT_CLEANSE.setString(entry.length + 1, "no flag");
	    		log_insertStmt += "'no flag'"; 
	    	}
	    	
	    	log.debug("ETL_LOGGER:(ps_INSERT_CLEANSE)= " + ps_INSERT_CLEANSE.toString());
	    	log.debug("ETL_LOGGER: " + log_insertStmt); //TODO: LOGGING
	    	try {
	    		ps_INSERT_CLEANSE.execute();
	    	} catch (Exception e1){
	    		if(errorCleanup(m_socj, save2, c, socj.currentFile, socj.faileddir_csvfiles, e1) == 2){
	    			break;
	    		}
	    	}
	    }
	}

	@Override
	public void copyFromCleanseToStaging(Connection c, SagesOpenCsvJar socj, Savepoint save2) throws SQLException, SagesEtlException {
		int lastComma;
		int lastTick;
		PreparedStatement ps_SELECT_CLEANSING = c.prepareStatement("SELECT * FROM " + socj.src_table_name);
		ResultSet rs_SELECT_CLEANSING = ps_SELECT_CLEANSING.executeQuery();
		
		
		PreparedStatement ps_SELECT_STAGING = c.prepareStatement("SELECT * FROM " + socj.dst_table_name);
		ResultSet rs_SELECT_STAGING = ps_SELECT_STAGING.executeQuery();
		ResultSetMetaData rsmd = rs_SELECT_STAGING.getMetaData();

		//ResultSetMetaData rsmd = rs_SELECT_CLEANSING.getMetaData();
		String[] rsColsHERE = new String[rsmd.getColumnCount()];
		
	      int numberOfColumns = rsmd.getColumnCount();
	      for (int x=1; x < numberOfColumns ; x++){
	    	  log.debug("LABEL: " + rsmd.getColumnLabel(x) + ", TYPE: " + rsmd.getColumnTypeName(x));
	    	  
	      }
	      for (Entry<String, String> dmap : socj.DEST_COLTYPE_MAP.entrySet()){
	    	  log.debug("LABEL: " + dmap.getKey() + ", TYPE: " + dmap.getValue());
	      }
	      
		for (int m=0; m <rsmd.getColumnCount(); m++){
			rsColsHERE[m] = rsmd.getColumnLabel(m+1);
		}

		// build the "INSERT INTO ETL_STAGING_DB" as reusable prepared statement
	    String stagingInsertStmt = "INSERT INTO " + socj.dst_table_name; 
	    String stagingColStmt = "(";
	    String stagingValuesStmt = " VALUES (";
	    for (String colHead: rsColsHERE){
	    	//createStmt += colHead + " " + sourceColTypeMap.get(colHead) + ",\n";
	    	stagingColStmt += colHead + ",\n";
	    }

	    stagingColStmt = StringUtils.substringBeforeLast(stagingColStmt, ",\n") + "\n)";
	    
	    for (int h=0;h<rsColsHERE.length;h++){
	    	stagingValuesStmt = stagingValuesStmt + "?,"; 
	    }
	    
    	stagingValuesStmt = StringUtils.substringBeforeLast(stagingValuesStmt, ",") + ");";
    	stagingInsertStmt = stagingInsertStmt + stagingColStmt + stagingValuesStmt;
    	
    	log.debug("!!!!stagingInsertStmt!!!!: \n" + stagingInsertStmt);
    	
    	/** Reusable Prepared Statement */
    	PreparedStatement ps_INSERT_STAGING = c.prepareStatement(stagingInsertStmt);
    	
		int z_colCount = rs_SELECT_CLEANSING.getMetaData().getColumnCount();
		ResultSetMetaData rsmd2 = rs_SELECT_CLEANSING.getMetaData();
		Map<String, Integer> z_indexMap = new HashMap<String, Integer>();
		
		ArrayList<String> alist = new ArrayList<String>(socj.MAPPING_MAP.values());
		Map<String, Integer> REV_INDX_MAPPING_MAP = new HashMap<String, Integer>();
		int zIndx = -1;
		for (int z = 0; z<z_colCount; z++){
			String currentColName = rsmd2.getColumnLabel(z+1);
			log.debug("currentColName: " + currentColName);
			if (socj.MAPPING_MAP.get(currentColName) != null){
				String destColName = socj.MAPPING_MAP.get(currentColName);
				log.debug("destColName: " + destColName);
				zIndx = alist.indexOf(destColName);
				z_indexMap.put(destColName, new Integer(zIndx));
			}
		}
		
		try {
	    	while (rs_SELECT_CLEANSING.next()){
	//    		for (int z = 0; z<z_colCount; z++){
	    		Set<Integer> masterindices_dst = new HashSet<Integer>(socj.PARAMINDX_DST.values());
	    		
	    		for (Entry<String,Integer> z_indexEntry: z_indexMap.entrySet()){
	    			
	    			//String currentColName = rs_SELECT_CLEANSING.
	    			//if (!MAPPING_MAP.containsKey(currentColName)) continue;
	    			String destColName = z_indexEntry.getKey();
	    			String sourcColName = socj.MAPPING_REV_MAP.get(destColName);
	    			log.debug("destcolNAME: "  + destColName);
	    			Integer destIndx = z_indexEntry.getValue();
	    			log.debug("destINDX: " + destIndx); //TODO: verify this zIndex make sure it's right
	    			Object VALUE = null;
	    				VALUE = rs_SELECT_CLEANSING.getObject(sourcColName);
					log.debug("THE VALUE AWAITED: "  + VALUE);
					
	    			Integer SQL_TYPE = socj.DEST_SQLTYPE_MAP.get(destColName);
	    			if (SQL_TYPE == Types.DATE){
	    				/** http://postgresql.1045698.n5.nabble.com/insert-from-a-select-td3279325.html */
	    				log.debug("date handling going on");
	    				DateFormat formatter;
	    				Date date;
	    				String formatToUse = socj.props_dateformats.getProperty(sourcColName).trim(); //i.e. "yyyy-MM-dd HH:mm:ss"
	    				formatter = new SimpleDateFormat(formatToUse);//grab configured date format
	    				try {
							date = (Date)formatter.parse(VALUE.toString());
							java.sql.Date sqlDate = new java.sql.Date(date.getTime());
							log.debug(sqlDate.toString());
							ps_INSERT_STAGING.setDate(socj.PARAMINDX_DST.get(destColName),sqlDate);
							log.debug("SET THE DATE STUFF-" + sqlDate);
							masterindices_dst.remove(socj.PARAMINDX_DST.get(destColName));
						} catch (ParseException e1) {
							// TODO Auto-generated catch block
							log.debug("ERROR: Check your date pattern in the file dateformats.properties:\n\t" +
									sourcColName + "=" + socj.props_dateformats.getProperty(sourcColName) +"\n");
							log.debug("ETL_LOGGER: error did occur for this file, but rolled back data and will proceed with next file.");
							e1.printStackTrace();
							//errorCleanup(save2, c, socj.currentFile, socj.faileddir_csvfiles, e1);
							throw new SagesEtlException(e1.getMessage(), e1);
						}
	    			} else {
	//    				ps_INSERT_STAGING.setObject(destIndx+1, VALUE, SQL_TYPE);
	    				ps_INSERT_STAGING.setObject(socj.PARAMINDX_DST.get(destColName), VALUE, SQL_TYPE);
	    				log.debug("SET NON DATE-"+ VALUE );
	    				masterindices_dst.remove(socj.PARAMINDX_DST.get(destColName));
	    			}
	    		}
	    		
	    		/** set NULLS for parameters with no values */
	    		for (Integer nullparamindx : masterindices_dst){
	    			ps_INSERT_STAGING.setNull(nullparamindx, rsmd.getColumnType(nullparamindx));
	    		}
	    		//TODO: NEED THIS NOT HARDCODED TO 10--should be 1+ number of columns...
	    		/** THIS IS FOR THE COLUMN ETL_FLAG **/
	    		ps_INSERT_STAGING.setNull(socj.PARAMINDX_DST.size() + 1, Types.VARCHAR);
	    		ps_INSERT_STAGING.executeUpdate();
	    	}
		}catch(SQLException se){
			log.debug("ETL_LOGGER: error did occur for this file, but rolled back data and will proceed with next file.");
			throw  SagesOpenCsvJar.abort(se.getMessage(), se);
		} catch (Exception e){
			log.debug("ETL_LOGGER: error did occur for this file, but rolled back data and will proceed with next file.");
			throw  SagesOpenCsvJar.abort(e.getMessage(), e);
		}
	}
	
	@Override
	public int errorCleanup(SagesOpenCsvJar socj, Savepoint savepoint, Connection connection, File currentCsv, String failedDirPath, Exception e){
    String savepointName = "";
    socj.success = false;
    File fileRefForStatusLogging = null;
    
    int errorFlag = 0;
		try {
			log.error("ERROR CLEANUP FOR EXCEPTION:\n" + e.getMessage());
			e.printStackTrace();
			savepointName = savepoint.getSavepointName();
			connection.rollback(savepoint);
			connection.commit();
			/** 
			 * MOVE CURRENT CSV OVER TO FAILED, 
			 * TODO: WRITE TO LOG: FILE_X FAILED, FAILURE OCCURED AT STEP_X **/
			if (currentCsv != null) {
				socj.failedCsvFiles.add(currentCsv);
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
