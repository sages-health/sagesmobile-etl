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
import java.util.Set;
import java.util.Map.Entry;

import org.apache.commons.io.FileUtils;
import org.jhuapl.edu.sages.etl.ETLProperties;
import org.jhuapl.edu.sages.etl.SagesEtlException;
import org.jhuapl.edu.sages.etl.oldstuff.TestOpenCsvJar;
import org.postgresql.util.PSQLException;

import au.com.bytecode.opencsv.CSVReader;

/**
 * @author POKUAM1
 * @created Nov 1, 2011
 */
public class ETLPostgresqlStrategy implements ETLStrategy {
	private TestOpenCsvJar m_tocj;
	private SagesOpenCsvJar m_socj;
	
	public ETLPostgresqlStrategy(SagesOpenCsvJar socj){
		m_socj = socj;
	}

	public ETLPostgresqlStrategy(TestOpenCsvJar tocj){
		m_tocj = tocj;
	}

	public ETLPostgresqlStrategy(){
		
	}
	
	/* (non-Javadoc)
	 * @see org.jhuapl.edu.sages.etl.strategy.ETLStrategy#extractHeaderColumns(org.jhuapl.edu.sages.etl.TestOpenCsvJar)
	 */
	@Override
	public void extractHeaderColumns(TestOpenCsvJar tocj) {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see org.jhuapl.edu.sages.etl.ETLStrategy#determineHeaderColumns(java.io.File)
	 */
	/**
	 * @param master_entries_rawdata
	 * @param file
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	@Override
	public String[] determineHeaderColumns(File file)throws FileNotFoundException, IOException {
		CSVReader reader_rawdata2 = new CSVReader(new FileReader(file));
		ArrayList<String[]> currentEntries = (ArrayList<String[]>) reader_rawdata2.readAll();
		String[] headerColumns = currentEntries.get(0); /** set header from the first csv file */
		reader_rawdata2.close();
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
	 * @see org.jhuapl.edu.sages.etl.ETLStrategy#buildCleanseTable(java.sql.Connection, org.jhuapl.edu.sages.etl.TestOpenCsvJar, java.sql.Savepoint)
	 */
	@Override
	public Savepoint buildCleanseTable(Connection c, TestOpenCsvJar tocj,
			Savepoint save1) throws SQLException, SagesEtlException {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see org.jhuapl.edu.sages.etl.ETLStrategy#alterCleanseTableAddFlagColumn(java.sql.Connection, java.sql.Savepoint, java.sql.Savepoint)
	 */
	/**
	 * @param c
	 * @param save1
	 * @param createCleanseSavepoint
	 * @throws SQLException
	 * @throws SagesEtlException
	 */
	@Override
	public void alterCleanseTableAddFlagColumn(Connection c,			
			Savepoint save1, Savepoint createCleanseSavepoint)
			throws SQLException, SagesEtlException {
		// TODO Auto-generated method stub
		String sqlaltertableAddColumn = addFlagColumn(m_socj.src_table_name);
	    PreparedStatement PS_addcolumn_Flag = c.prepareStatement(sqlaltertableAddColumn);
	    System.out.println("ALTER STATEMENT: " + sqlaltertableAddColumn);
	    //TODO catch that column already exists
	    try {
	    	PS_addcolumn_Flag.execute();
	    } catch (PSQLException e){
	    	if ("25P02".equals(e.getSQLState())){
	    		System.out.println("ETL_LOGGER(ignored but rolling back to "+ createCleanseSavepoint.getSavepointName() + "):" + e.getSQLState() + ", " + e.getMessage()); //TODO: LOGGING
	    		c.rollback(createCleanseSavepoint);
	    	}else if ("42701".equals(e.getSQLState())){
	    		System.out.println("ETL_LOGGER(ignored):" + e.getSQLState() + ", " + e.getMessage()); //TODO: LOGGING
	    		c.rollback(createCleanseSavepoint);
	    	} else {
	    		errorCleanup(save1, c, null, m_socj.faileddir_csvfiles, e);
	    		throw SagesOpenCsvJar.abort("Uh-oh, something happened trying to add column etl_flag to ETL_CLEANSING_TABLE.", e); 
	    	}
	    } catch (SQLException e){ //TODO MS Access specific
	    	if ("S0021".equals(e.getSQLState())){
	    		System.out.println("ETL_LOGGER:" + e.getSQLState() + ", " + e.getMessage()); //TODO: LOGGING
	    	} else {
	    		errorCleanup(save1, c, null, m_socj.faileddir_csvfiles, e);
	    		throw SagesOpenCsvJar.abort("Uh-oh, something happened trying to add column etl_flag to ETL_CLEANSING_TABLE.", e); 
	    	}
	    } catch (Exception e) {
	    	errorCleanup(save1, c, null,  m_socj.faileddir_csvfiles, e);
	    	throw SagesOpenCsvJar.abort("Uh-oh, something happened trying to add column etl_flag to ETL_CLEANSING_TABLE.", e); 
	    }
	}

	
	/* (non-Javadoc)
	 * @see org.jhuapl.edu.sages.etl.ETLStrategy#buildStagingTable(java.sql.Connection, org.jhuapl.edu.sages.etl.TestOpenCsvJar, java.sql.Savepoint)
	 */
	@Override
	public void buildStagingTable(Connection c, TestOpenCsvJar tocj,
			Savepoint save1) throws SQLException, SagesEtlException {
		// TODO Auto-generated method stub
		
	}

	
	/* (non-Javadoc)
	 * @see org.jhuapl.edu.sages.etl.ETLStrategy#alterStagingTableAddFlagColumn(java.sql.Connection, java.sql.Savepoint, java.sql.Savepoint)
	 */
	/**
	 * @param c
	 * @param save1
	 * @param createCleanseSavepoint
	 * @throws SQLException
	 * @throws SagesEtlException
	 */
	@Override
	public void alterStagingTableAddFlagColumn(Connection c, Savepoint save1,
			Savepoint createCleanseSavepoint) throws SQLException,
			SagesEtlException {

		String sqlaltertableAddColumn;
		PreparedStatement PS_addcolumn_Flag;
		sqlaltertableAddColumn = addFlagColumn(m_socj.dst_table_name);
	    PS_addcolumn_Flag = c.prepareStatement(sqlaltertableAddColumn);
	    System.out.println("ALTER STATEMENT: " + sqlaltertableAddColumn);
	    try {
	    	PS_addcolumn_Flag.execute();
	    } catch (PSQLException e){
	    	/** catch that column already exists. that is OK */
	    	if ("42701".equals(e.getSQLState())){
	    		System.out.println("ETL_LOGGER:" + e.getSQLState() + ", " + e.getMessage()); //TODO: LOGGING
	    		c.rollback(createCleanseSavepoint);
	    	} else {
	    		errorCleanup(save1, c, null, m_socj.faileddir_csvfiles, e);
	    		throw SagesOpenCsvJar.abort("Uh-oh, something happened trying to add column etl_flag to ETL_STAGING_DB.", e); 
	    	}
	    } catch (SQLException e){ //TODO MS Access specific
	    	if ("S0021".equals(e.getSQLState())){
	    		System.out.println("ETL_LOGGER:" + e.getSQLState() + ", " + e.getMessage()); //TODO: LOGGING
	    	} else {
	    		errorCleanup(save1, c, null, m_socj.faileddir_csvfiles, e);
	    		throw SagesOpenCsvJar.abort("Uh-oh, something happened trying to add column etl_flag to ETL_CLEANSING_TABLE.", e); 
	    	}
	    } catch (Exception e) {
	    	errorCleanup(save1, c, null, m_socj.faileddir_csvfiles, e);
	    	throw SagesOpenCsvJar.abort("Uh-oh, something happened trying to add column etl_flag to ETL_STAGING_DB.", e); 
	    }
	}

	/* (non-Javadoc)
	 * @see org.jhuapl.edu.sages.etl.ETLStrategy#generateSourceDestMappings(org.jhuapl.edu.sages.etl.TestOpenCsvJar)
	 */
	@Override
	public void generateSourceDestMappings(TestOpenCsvJar tocj) {
		// TODO Auto-generated method stub
		
	}
	
	/********ORIGINALS ABOVE, DOWN IS SOCJ STUFF *******************/

	/* (non-Javadoc)
	 * @see org.jhuapl.edu.sages.etl.strategy.ETLStrategy#extractHeaderColumns(org.jhuapl.edu.sages.etl.opencsvpods.SagesOpenCsvJar)
	 */
	/**
	 * @param tocj
	 * @return
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	@Override
	public void extractHeaderColumns(SagesOpenCsvJar socj) throws FileNotFoundException, IOException {
		/** header columns used to define the CLEANSE table schema */
		socj.header_src = new String[0];
		//failedCsvFiles = new ArrayList<File>();
		//File[] readCsvFiles = new File[0];
		
		/** determine header columns **/
		File csvinputDir = new File(socj.inputdir_csvfiles);
		if (csvinputDir.isDirectory()){
			socj.csvFiles = csvinputDir.listFiles();
			if (socj.csvFiles.length == 0){
				System.out.println("Nothing to load into database. Exiting."); //TODO: LOGGING explain this happens
				System.exit(0); 
			} 
			socj.header_src = determineHeaderColumns(socj.csvFiles[0]);
		} else {
			System.out.println(csvinputDir.getName() + "is not valid csv input directory. Exiting."); //TODO: LOGGING explain this happens
			System.exit(0); 	
		}
		//return readCsvFiles;
		
	}

	
	/* (non-Javadoc)
	 * @see org.jhuapl.edu.sages.etl.strategy.ETLStrategy#buildCleanseTable(java.sql.Connection, org.jhuapl.edu.sages.etl.opencsvpods.SagesOpenCsvJar, java.sql.Savepoint)
	 */
	/**
	 * @param c
	 * @param tocj
	 * @param save1
	 * @return
	 * @throws SQLException
	 * @throws SagesEtlException
	 */
	@Override
	public Savepoint buildCleanseTable(Connection c, SagesOpenCsvJar socj, Savepoint save1) throws SagesEtlException, SQLException {
		/*********************************** 
	     * build ETL_CLEANSE_TABLE 
	     ***********************************
	     * SQL: "CREATE TABLE..." 
	     * - all columns have text sql-datatype
	     * - column definitions built from csv file header 
	     ****************************/
	     //http://postgresql.1045698.n5.nabble.com/25P02-current-transaction-is-aborted-commands-ignored-until-end-of-transaction-block-td2174290.html
		socj.src_table_name = socj.props_etlconfig.getProperty("dbprefix_src") + "_" + socj.ETL_CLEANSE_TABLE;
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
	   
	    /**remove trailing ',' TODO CLEAN UP WITH StringUtils.join() */
	    int lastComma = createStmt_src.lastIndexOf(",\n");
	    createStmt_src = createStmt_src.substring(0, lastComma);
	    createStmt_src += "\n);";
	    
	    System.out.println("ETL_LOGGER\ncreatestmt:\n" + createStmt_src); //TODO: LOGGING
	    PreparedStatement PS_create_CLEANSE = c.prepareStatement(createStmt_src);
	    
	    Savepoint createCleanseSavepoint = c.setSavepoint("createCleanseSavepoint");
	    try {
	    	PS_create_CLEANSE.execute(); //TODO: pgadmin wanted a pk for me to edit thru gui
	    } catch (PSQLException e){ //TODO: make this generic for SQLException
	    	if ("42P07".equals(e.getSQLState())){
	    		System.out.println("ETL_LOGGER:" + e.getSQLState() + ", " + e.getMessage()); //TODO: LOGGING
	    		c.rollback(createCleanseSavepoint);
	    	} else {
	    		errorCleanup(save1, c, null, socj.faileddir_csvfiles, e);
	    		throw socj.abort("Uh-oh, something happened trying to build the ETL_CLEANSING_TABLE.", e); 
	    	}
	    } catch (SQLException e){ //TODO: make this generic for SQLException this is MS Access error
	    	if ("S0001".equals(e.getSQLState())){
	    		System.out.println("ETL_LOGGER:" + e.getSQLState() + ", " + e.getMessage()); //TODO: LOGGING
	    	} else {
	    		errorCleanup(save1, c, null, socj.faileddir_csvfiles, e);
	    		throw socj.abort("Uh-oh, something happened trying to build the ETL_CLEANSING_TABLE.", e); 
	    	}
	    } catch (Exception e) {
	    	errorCleanup(save1, c, null, socj.faileddir_csvfiles, e);
	    	throw socj.abort("Uh-oh, something happened trying to build the ETL_CLEANSING_TABLE.", e); 
	    }
		return createCleanseSavepoint;
	}
	
	/* (non-Javadoc)
	 * @see org.jhuapl.edu.sages.etl.strategy.ETLStrategy#buildStagingTable(java.sql.Connection, org.jhuapl.edu.sages.etl.opencsvpods.SagesOpenCsvJar, java.sql.Savepoint)
	 */
	/**
	 * @param c
	 * @param tocj
	 * @param save1
	 * @return Savepoint
	 * @throws SQLException
	 * @throws SagesEtlException
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
		String tableNamePattern = socj.props_etlconfig.getProperty("tableNamePattern"); //TODO rename this
		String columnNamePattern = null;
		
		ResultSet rs_FINAL = dbmd.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);

		String destTableStr = "";
		socj.dst_table_name = socj.props_etlconfig.getProperty("dbprefix_dst") + "_" + socj.ETL_STAGING_DB;
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
			//System.out.println("column check");
			String colName = rs_FINAL.getString("COLUMN_NAME");
			String colType = rs_FINAL.getString("TYPE_NAME");
			int colSqlType = rs_FINAL.getInt("DATA_TYPE");
			String isAutoInc = "";
			try {
				isAutoInc = rs_FINAL.getString("IS_AUTOINCREMENT");  /**YES, NO, or "" */
			} catch (SQLException e) {
				if ("S0022".equals(e.getSQLState())){ // MS Access specific
					System.out.println("ETL_LOGGER:" + "this database does not support IS_AUTOINCREMENT result meta data column. safe to ignore");
				}
			}
			if ("serial".equalsIgnoreCase(colType) || "COUNTER".equalsIgnoreCase(colType)|| "YES".equalsIgnoreCase(isAutoInc)) {
				continue;
			}
			
			String colDef = colName + " " + colType + ",";
			destTableStr += colDef + "\n";

			System.out.println(colName + "=" + colType + "("+ colSqlType + ")"); //TODO: LOGGING
			/** TODO: make sure using this staging table map <colName:colType>  **/
			socj.DEST_COLTYPE_MAP.put(colName, colType);
			socj.DEST_SQLTYPE_MAP.put(colName, colSqlType);
			socj.PARAMINDX_DST.put(colName, zdst);
	    	zdst++;
		}
		
		/** the built "CREATE TABLE STAGING_TABLE..." string */
	    Savepoint createStagingSavepoint = c.setSavepoint("createStagingSavePoint");
		/** remove trailing ','  TODO CLEAN UP WITH StringUtils.join()  */
	    int lastTick = destTableStr.lastIndexOf(",");
	    destTableStr = destTableStr.substring(0, lastTick);
	    String createStagingStmt = "CREATE TABLE " + socj.dst_table_name + "\n(\n" + destTableStr + "\n);";
		System.out.println(createStagingStmt); //TODO: LOGGING
		
		PreparedStatement ps_CREATE_STAGING = c.prepareStatement(createStagingStmt);
		
		try {
			/** execute CREATE STAGING_TABLE sql */
			ps_CREATE_STAGING.execute();
		} catch (PSQLException e){ //TODO: make this generic for SQLException
	    	if ("42P07".equals(e.getSQLState())){
	    		System.out.println("ETL_LOGGER:(ignored)" + e.getSQLState() + ", " + e.getMessage()); //TODO: LOGGING
	    		c.rollback(createStagingSavepoint);
	    	} else {
	    		errorCleanup(save1, c, null, socj.faileddir_csvfiles, e);
	    		throw SagesOpenCsvJar.abort("Uh-oh, something happened trying to build the ETL_STAGING_DB.", e); 
	    	}
	    } catch (SQLException e){ //TODO: make this generic for SQLException this is MS Access error
	    	if ("S0001".equals(e.getSQLState())){
	    		System.out.println("ETL_LOGGER:" + e.getSQLState() + ", " + e.getMessage()); //TODO: LOGGING
	    	} else {
	    		errorCleanup(save1, c, null, socj.faileddir_csvfiles, e);
	    		throw SagesOpenCsvJar.abort("Uh-oh, something happened trying to build the ETL_CLEANSING_TABLE.", e); 
	    	}
	    } catch (Exception e) {
	    	errorCleanup(save1, c, null, socj.faileddir_csvfiles, e);
	    	throw SagesOpenCsvJar.abort("Uh-oh, something happened trying to build the ETL_STAGING_DB.", e); 
	    }
		return createStagingSavepoint; 
	}

	
	/* (non-Javadoc)
	 * @see org.jhuapl.edu.sages.etl.strategy.ETLStrategy#generateSourceDestMappings(org.jhuapl.edu.sages.etl.opencsvpods.SagesOpenCsvJar)
	 */
	/**
	 * @param socj
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
	
	/**
	 * @param c
	 * @param socj
	 * @return
	 * @throws SQLException
	 */
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
    	lastTick = insertStmt_src.lastIndexOf(",");
    	insertStmt_src = insertStmt_src.substring(0, lastTick);
    	insertStmt_src += ");";
    	
    	System.out.println("ETL_LOGGER\ninsertstmt_src: " + insertStmt_src); //TODO: LOGGING
    	return insertStmt_src;
	}

	/**
	 * @param c
	 * @param socj
	 * @param entries_rawdata
	 * @param save2
	 * @param ps_INSERT_CLEANSE
	 * @throws SQLException
	 */
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
	    	
	    	System.out.println("ETL_LOGGER:(ps_INSERT_CLEANSE)= " + ps_INSERT_CLEANSE.toString());
	    	System.out.println("ETL_LOGGER: " + log_insertStmt); //TODO: LOGGING
	    	try {
	    		ps_INSERT_CLEANSE.execute();
	    	} catch (Exception e1){
	    		if(errorCleanup(save2, c, null, socj.faileddir_csvfiles, e1) == 2){
	    			break;
	    		}
	    	}
	    }
	}

	/**
	 * @param c
	 * @param tocj
	 * @param save2
	 * @throws SQLException
	 */
	public void copyFromCleanseToStaging(Connection c, SagesOpenCsvJar socj, Savepoint save2) throws SQLException {
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
	    	  System.out.println("LABEL: " + rsmd.getColumnLabel(x) + ", TYPE: " + rsmd.getColumnTypeName(x));
	    	  
	      }
	      for (Entry<String, String> dmap : socj.DEST_COLTYPE_MAP.entrySet()){
	    	  System.out.println("LABEL: " + dmap.getKey() + ", TYPE: " + dmap.getValue());
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
	    lastComma = stagingColStmt.lastIndexOf(",\n");
	    stagingColStmt = stagingColStmt.substring(0, lastComma);
	    stagingColStmt += "\n)";
	    
	    for (int h=0;h<rsColsHERE.length;h++){
	    	stagingValuesStmt = stagingValuesStmt + "?,"; 
	    }
    	lastTick = stagingValuesStmt.lastIndexOf(",");
    	stagingValuesStmt = stagingValuesStmt.substring(0, lastTick);
    	stagingValuesStmt += ");";
    	stagingInsertStmt = stagingInsertStmt + stagingColStmt + stagingValuesStmt;
    	
    	System.out.println("!!!!stagingInsertStmt!!!!: \n" + stagingInsertStmt);
    	
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
			System.out.println("currentColName: " + currentColName);
			if (socj.MAPPING_MAP.get(currentColName) != null){
				String destColName = socj.MAPPING_MAP.get(currentColName);
				System.out.println("destColName: " + destColName);
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
	    			System.out.println("destcolNAME: "  + destColName);
	    			Integer destIndx = z_indexEntry.getValue();
	    			System.out.println("destINDX: " + destIndx); //TODO: verify this zIndex make sure it's right
	    			Object VALUE = rs_SELECT_CLEANSING.getObject(sourcColName);
					System.out.println("THE VALUE AWAITED: "  + VALUE);
					
	    			Integer SQL_TYPE = socj.DEST_SQLTYPE_MAP.get(destColName);
	    			if (SQL_TYPE == Types.DATE){
	    				/** http://postgresql.1045698.n5.nabble.com/insert-from-a-select-td3279325.html */
	    				System.out.println("date handling going on");
	    				DateFormat formatter;
	    				Date date;
	    				String formatToUse = socj.props_dateformats.getProperty(sourcColName).trim(); //i.e. "yyyy-MM-dd HH:mm:ss"
	    				formatter = new SimpleDateFormat(formatToUse);//grab configured date format
	    				try {
							date = (Date)formatter.parse(VALUE.toString());
							java.sql.Date sqlDate = new java.sql.Date(date.getTime());
							System.out.println(sqlDate.toString());
							ps_INSERT_STAGING.setDate(socj.PARAMINDX_DST.get(destColName),sqlDate);
							System.out.println("SET THE DATE STUFF-" + sqlDate);
							masterindices_dst.remove(socj.PARAMINDX_DST.get(destColName));
						} catch (ParseException e1) {
							// TODO Auto-generated catch block
							System.out.println("ERROR: Check your date pattern in the file dateformats.properties:\n\t" +
									sourcColName + "=" + socj.props_dateformats.getProperty(sourcColName) +"\n");
							e1.printStackTrace();
							errorCleanup(save2, c, null, socj.faileddir_csvfiles, e1);
							throw new SagesEtlException("Cannot proceed errors");
						}
	    			} else {
	//    				ps_INSERT_STAGING.setObject(destIndx+1, VALUE, SQL_TYPE);
	    				ps_INSERT_STAGING.setObject(socj.PARAMINDX_DST.get(destColName), VALUE, SQL_TYPE);
	    				System.out.println("SET NON DATE-"+ VALUE );
	    				masterindices_dst.remove(socj.PARAMINDX_DST.get(destColName));
	    			}
	    		}
	    		
	    		/** set NULLS for parameters with no values */
	    		for (Integer nullparamindx : masterindices_dst){
	    			ps_INSERT_STAGING.setNull(nullparamindx, rsmd.getColumnType(nullparamindx));
	    		}
	    		//TODO: NEED THIS NOT HARDCODED TO 10--should be 1+ number of columns...
	    		ps_INSERT_STAGING.setNull(socj.PARAMINDX_DST.size() + 1, Types.VARCHAR);
	    		ps_INSERT_STAGING.executeUpdate();
	    	}
		} catch (Exception e){
			errorCleanup(save2, c, null, socj.faileddir_csvfiles, e);
			System.out.println("ETL_LOGGER: error did occur for this file, but rolled back and proceeding.");
		}
	}
	
	public int errorCleanup(Savepoint savepoint, Connection connection, File currentCsv, String failedDirPath, Exception e){
    String savepointName = "";
    int errorFlag = 0;
		try {
			System.out.println("ERROR OCCURED THIS IS ERROR CLEANUP FOR exception:\n" + e.getMessage());
			savepointName = savepoint.getSavepointName();
			connection.rollback(savepoint);
			connection.commit();
			//MOVE CURRENT CSV OVER TO FAILED
			//WRITE TO LOG: FILE_X FAILED, FAILURE OCCURED AT STEP_X
			if (currentCsv != null) {
				//failedCsvFiles.add(currentCsv);
		    	Date date = new Date();
		    	long dtime = date.getTime();
		    	//** Destination dir *//*
		    	File dir = new File(failedDirPath);
		    	//** Move file to new dir *//*
		    	FileUtils.copyFile(currentCsv, new File(dir, dtime + "_"+ currentCsv.getName()));
		    	//boolean success = file.renameTo(new File(dir, dtime + "_"+ file.getName()));
		    	Thread.sleep(2000);
		    	FileUtils.forceDelete(currentCsv);
			}
		} catch(IOException io){
			System.out.println("ugh: " + io.getMessage());
		}catch (InterruptedException ie){
			System.out.println("ugh: " + ie.getMessage());
		}catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} finally {
			System.out.println("SYSTEM ROLLED BACK TO SAVEPOINT = " + savepointName);
			if ("save1".equals(savepointName)){
				errorFlag = 1;
				System.exit(-1);
			} if ("save2".equals(savepointName)){
				errorFlag = 2;
			}
	}
	 return errorFlag;	
	}


}
