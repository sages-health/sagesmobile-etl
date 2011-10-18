package org.jhuapl.edu.sages.etl;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.postgresql.util.PSQLException;

import au.com.bytecode.opencsv.CSVReader;

/**
 * @author POKUAM1
 * @created Oct 4, 2011
 */
public class TestOpenCsvJar {

	private static final String ETL_CLEANSE_TABLE = "ETL_CLEANSE_TABLE";
	private static final String ETL_STAGING_DB = "ETL_STAGING_DB";
	private static String src_table_name;
	private static String dst_table_name;
	/** maps the destination columns to their sql-datatype qualifier for generating the schema */
	private static Map<String,String> DEST_COLTYPE_MAP;
	
	/** maps the destination columns to their java.sql.Types for setting ? parameters on prepared statements */
	private static Map<String, Integer> DEST_SQLTYPE_MAP;
	
	//http://download.oracle.com/javase/6/docs/api/constant-values.html#java.sql.Types.TIME
		
	/** properties holders */
	private Properties props = new Properties();
	private Properties props_dateformats = new Properties();
	private Properties props_customsql_cleanse = new Properties();
	private Properties props_customsql_staging = new Properties();
		
	/** target database connection settings*/
	private String dbms;
	private int portNumber;
	private String serverName;
	private String dbName;
	private String userName;
	private String password;
		
	/** constants for activated database specific features */
	static final String dbid_mysql = "mysql";
	static final String dbid_postgresql = "postgresql";
	static final String dbid_derby = "derby";
	static final String dbid_msaccess = "msaccess";
		
	/** maps source column name to its parameter index in the source table, indexing starts at 1 */
	private static Map<String,Integer> PARAMINDX_SRC = new HashMap<String,Integer>();
	/** maps destination column name to its parameter index in the destination table, indexing starts at 1 */
	private static Map<String,Integer> PARAMINDX_DST = new HashMap<String,Integer>();
		
	/** header columns used to define the CLEANSE table schema */
	private static String[] header_src = new String[0];
	/**
	 * @param dbms
	 * @param portNumber
	 * @param userName
	 * @param password
	 * @param serverName
	 * @param dbName
	 */
	public TestOpenCsvJar(String dbms, int portNumber, String userName,
			String password, String serverName, String dbName) {
		super();
		this.props = new Properties();
		this.dbms = dbms;
		this.portNumber = portNumber;
		this.userName = userName;
		this.password = password;
		this.serverName = serverName;
		this.dbName = dbName;
	}
		
	/**
	 * @param dbms
	 * @param portNumber
	 * @param userName
	 * @param password
	 * @param serverName
	 * @param dbName
	 * @throws SagesEtlException 
	 */
	public TestOpenCsvJar() throws SagesEtlException {
		super();
		try {
			this.props = new Properties();
			this.props_dateformats = new Properties();
			this.props_customsql_cleanse = new Properties();
			this.props_customsql_staging = new Properties();
			this.props.load(new FileInputStream("etlconfig.properties"));
			this.props_dateformats.load(new FileInputStream("dateformats.properties"));
			this.props_customsql_cleanse.load(new FileInputStream("customsql\\cleanse_table\\cleanse_sql.properties"));
			this.props_customsql_staging.load(new FileInputStream("customsql\\staging_table\\staging_sql.properties"));
		} catch (IOException e){
			//TODO: LOG THIS ERROR
			e.printStackTrace();
			throw new SagesEtlException("Problem occurred loading properties. Check that properties files exist", e);
		}
		
		this.dbms = props.getProperty("dbms").trim();
		this.portNumber = Integer.valueOf(props.getProperty("portNumber")).intValue();
		this.userName = props.getProperty("userName").trim();
		this.password = props.getProperty("password").trim();
		this.serverName = props.getProperty("serverName").trim();
		this.dbName = props.getProperty("dbName").trim();
	}
		
	/**
	 * Establishes database connection to the target database
	 * @return Connection
	 * @throws SQLException
	 */
	public Connection getConnection() throws SQLException {
	    Connection con = null;
	    
	    Properties connectionProps = new Properties();
	    connectionProps.put("user", this.userName);
	    connectionProps.put("password", this.password);

	    if (this.dbms.equals(dbid_mysql)) {
	    	con = DriverManager.getConnection("jdbc:" + this.dbms + "://" + this.serverName + ":" + this.portNumber + "/", connectionProps);
	    } else if (this.dbms.equals(dbid_derby)) {
	    	con = DriverManager.getConnection("jdbc:" + this.dbms + ":" + this.dbName + ";create=true", connectionProps);
	    } else if (this.dbms.equals(dbid_postgresql)) {
	    	con = DriverManager.getConnection("jdbc:" + this.dbms + "://" + this.serverName + ":" + portNumber + "/" + this.dbName, connectionProps);
	    }
	    //TODO: DO AS LOGGING
	    System.out.println("Connected to database");
	    return con;
    }

	/**
	 * returns a SagesEtlException that wraps the original exception	
	 * @param msg SAGES ETL message to display
	 * @param e the original exception
	 * @return SagesEtlException
	 */
	public static SagesEtlException abort(String msg, Throwable e){
		return new SagesEtlException(msg, e);
	} 
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
//		TestOpenCsvJar tocj = new TestOpenCsvJar("postgresql",5432,"postgres","pgPOKU123!","localhost","ETL_TEST");
		TestOpenCsvJar tocj = new TestOpenCsvJar();
		Connection c = null; 
		try {
			c = tocj.getConnection();
			System.out.println("catalog: " + c.getCatalog());
			
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			try {
				c.close();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				//e1.printStackTrace();
				throw abort("Sorry, failed to establish database connection", e);
			} catch (Exception e2) {
				
				//e.printStackTrace();
				throw abort("Sorry, failed to establish database connection", e);
			}
			//e.printStackTrace();
			throw abort("Sorry, failed to establish database connection", e);
		}
		
		
		CSVReader reader_rawdata = new CSVReader(new FileReader("C:\\dev\\git-repositories\\sandbox\\sandbox\\unittests\\datachunks\\modded_formdata_oevisit11106-1715.csv"));

		/** Reading multiple CSV files:
		 * 
		 * -1- get list of .csv files
		 * -2- create a CSVReader from each file
		 * -3- CSVReader.readALL() on each, and take readentries[1 to end] and append it to the "Master" readentries list.

				ArrayList<String[]> MASTER_entries_rawdata = new ArrayList<String[]>();
				MASTER_entries_rawdata.addAll(null);
		
		 * -4- if readentries are sql loaded OK, move csv files to different directory
		       http://stackoverflow.com/questions/300559/move-copy-file-operations-in-java
			   http://www.exampledepot.com/egs/java.io/MoveFile.html
				-INPUTDIR for CSV defined in etlconfig.properties
				 csvinputdir=csvfilerepo\\in
				
				-OUTPUTDIR for CSV processed defined in etlconfig.properties
				 csvoutputdir=csvfilerepo\\out
		   
		   -5- TODO: if readentries are sql loaded FAILURE, ??????
		 *	
		 * 
		 */ 
		
		/** csv files are loaded from inputdir and moved to outputdir after being processed successfully  */
		String inputdir_csvfiles = tocj.props.getProperty("csvinputdir");
		String outputdir_csvfiles = tocj.props.getProperty("csvoutputdir");
		
		ArrayList<String[]> master_entries_rawdata = new ArrayList<String[]>();

		/** header columns used to define the CLEANSE table schema */
		header_src = new String[0];
		File[] readCsvFiles = new File[0];
		
		/** load into memory all csv files in the inputdir */
		File csvinputDir = new File(inputdir_csvfiles);
		if (csvinputDir.isDirectory()){
			File[] csvFiles = csvinputDir.listFiles();
			if (csvFiles.length == 0){
				System.out.println("Nothing to load into database. Exiting."); //TODO: LOGGING explain this happens
				System.exit(0); 
			} 
				
			int f = 0; /** to grab the header from the first csv file */
			for (File file : csvFiles){
				ArrayList<String[]> currentEntries;
				if (f >= 1){
					CSVReader reader_rawdata2 = new CSVReader(new FileReader(file));
					currentEntries = (ArrayList<String[]>) reader_rawdata2.readAll();
					currentEntries.remove(0);  /** remove the header row */
					master_entries_rawdata.addAll(currentEntries);
				} else {
					CSVReader reader_rawdata2 = new CSVReader(new FileReader(file));
					currentEntries = (ArrayList<String[]>) reader_rawdata2.readAll();
					header_src = currentEntries.get(0); /** set header from the first csv file */
					currentEntries.remove(0); /** remove the header row */
					master_entries_rawdata.addAll(currentEntries);
					f++;
				}
			}
			readCsvFiles = csvFiles;
		}
		
	    ArrayList<String[]> entries_rawdata = master_entries_rawdata;
	    
	    /*********************************** 
	     * build ETL_CLEANSE_TABLE 
	     ***********************************
	     * SQL: "CREATE TABLE..." 
	     * - all columns have text sql-datatype
	     * - column definitions built from csv file header 
	     ****************************/
	    src_table_name = tocj.props.getProperty("dbprefix_src") + "_" + ETL_CLEANSE_TABLE;
	    String createStmt_src = "CREATE TABLE " + src_table_name + " \n(\n";
	    
	    /**
	     * build schema definition & map column to its index for later use with ResultSet and PreparedStatment
	     */
	    int zsrc=1;
	    for (String colHead_src: header_src){
	    	createStmt_src += colHead_src + " varchar(255),\n";
	    	PARAMINDX_SRC.put(colHead_src, zsrc);
	    	zsrc++;
	    }
	   
	    /**remove trailing ',' */
	    int lastComma = createStmt_src.lastIndexOf(",\n");
	    createStmt_src = createStmt_src.substring(0, lastComma);
	    createStmt_src += "\n);";
	    
	    System.out.println("ETL_LOGGER\ncreatestmt:\n" + createStmt_src); //TODO: LOGGING
	    PreparedStatement PS_create_CLEANSE = c.prepareStatement(createStmt_src);
	    
	    try {
	    	PS_create_CLEANSE.execute(); //TODO: pgadmin wanted a pk for me to edit thru gui
	    } catch (PSQLException e){ //TODO: make this generic for SQLException
	    	if ("42P07".equals(e.getSQLState())){
	    		System.out.println("ETL_LOGGER:" + e.getSQLState() + ", " + e.getMessage()); //TODO: LOGGING
	    	} else {
	    		throw abort("Uh-oh, something happened trying to build the ETL_CLEANSING_TABLE.", e); 
	    	}
	    } catch (Exception e) {
	    	throw abort("Uh-oh, something happened trying to build the ETL_CLEANSING_TABLE.", e); 
	    }
	    
		String sqlaltertableAddColumn = addFlagColumn(src_table_name);
	    PreparedStatement PS_addcolumn_Flag = c.prepareStatement(sqlaltertableAddColumn);
	    System.out.println("ALTER STATEMENT: " + sqlaltertableAddColumn);
	    //TODO catch that column already exists
	    try {
	    	PS_addcolumn_Flag.execute();
	    } catch (PSQLException e){
	    	if ("42701".equals(e.getSQLState())){
	    		System.out.println("ETL_LOGGER:" + e.getSQLState() + ", " + e.getMessage()); //TODO: LOGGING
	    	} else {
	    		throw abort("Uh-oh, something happened trying to add column etl_flag to ETL_CLEANSING_TABLE.", e); 
	    	}
	    } catch (Exception e) {
	    	throw abort("Uh-oh, something happened trying to add column etl_flag to ETL_CLEANSING_TABLE.", e); 
	    }
	    
  
	    
	    /************************************************ 
	     * build reusable 'INSERT INTO CLEANSING_TABLE'
	     ************************************************
	     * SQL: "INSERT INTO SRC_TABLE..."
	     * Example SQL: "INSERT INTO src_table_name VALUES (?, ?, ?, ?, ?,...)"
	     * data will be inserted as text sql-datatype
	     *  
	     */
	    
	    String insertStmt_src = "INSERT INTO " + src_table_name + " VALUES (";
	    
	    for (int h=0; h < header_src.length; h++){
	    	insertStmt_src = insertStmt_src + "?,"; 
	    }
	    
	    /** remove trailing ','   */
    	int lastTick = insertStmt_src.lastIndexOf(",");
    	insertStmt_src = insertStmt_src.substring(0, lastTick);
    	insertStmt_src += ");";
    	
    	System.out.println("ETL_LOGGER\ninsertstmt_src: " + insertStmt_src); //TODO: LOGGING
    	PreparedStatement ps_INSERT_CLEANSE = c.prepareStatement(insertStmt_src);
	    	
    	/** set values for the ? parameters, NOTE all values have text sql-datatype */
	    for (int e=0; e < entries_rawdata.size(); e++){
	    	String[] entry = entries_rawdata.get(e);
	    	String log_insertStmt = "VALUES:"; //TODO: LOGGING
	    	
	    	for (int p=0; p < entry.length; p++){
	    		ps_INSERT_CLEANSE.setString(p+1, entry[p]);
	    		log_insertStmt += "'" + entry[p] + "',"; 
	    	}
	    	
	    	ps_INSERT_CLEANSE.execute();
	    	System.out.println("ETL_LOGGER: " + log_insertStmt); //TODO: LOGGING
	    }
	    
	    /** TODO
	     * 
	     * !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	     * INJECT THE CUSTOM SQL AGAINST THE CLEANSE TABLE HERE
	     * !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	     * 
	     * */
    	Properties customCleanseSqlprops = tocj.props_customsql_cleanse;
    	int numSql = customCleanseSqlprops.size();
    	for (int i = 1; i <= numSql; i++){
    		String sql = customCleanseSqlprops.getProperty(String.valueOf(i));
    		sql = sql.replace("$table", src_table_name);
    		System.out.println("CUSTOM SQL: " + sql);
    		PreparedStatement ps = c.prepareStatement(sql);
    		ps.execute();
    	}
    	
	    
	    /** get metadata for FINAL DESTINATION TABLE and use it to build STAGING */
		DatabaseMetaData dbmd = c.getMetaData();
		String catalog = null;
		String schemaPattern = null;
		//String tableNamePattern = "etl_individual";
		String tableNamePattern = tocj.props.getProperty("tableNamePattern");
		String columnNamePattern = null;
		
		ResultSet rs_FINAL = dbmd.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);

		String destTableStr = "";
		dst_table_name = tocj.props.getProperty("dbprefix_dst") + "_" + ETL_STAGING_DB;
		DEST_COLTYPE_MAP = new LinkedHashMap<String, String>();
		DEST_SQLTYPE_MAP = new LinkedHashMap<String, Integer>();
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
			String isAutoInc = rs_FINAL.getString("IS_AUTOINCREMENT");  /**YES, NO, or "" */
			if ("serial".equals(colType) || "YES".equals(isAutoInc)) {
				continue;
			}
			
			String colDef = colName + " " + colType + ",";
			destTableStr += colDef + "\n";

			System.out.println(colName + "=" + colType + "("+ colSqlType + ")"); //TODO: LOGGING
			/** TODO: make sure using this staging table map <colName:colType>  **/
			DEST_COLTYPE_MAP.put(colName, colType);
			DEST_SQLTYPE_MAP.put(colName, colSqlType);
	    	PARAMINDX_DST.put(colName, zdst);
	    	zdst++;
		}
		
		/** the built "CREATE TABLE STAGING_TABLE..." string */
	    
		/** remove trailing ','   */
	    lastTick = destTableStr.lastIndexOf(",");
	    destTableStr = destTableStr.substring(0, lastTick);
	    String createStagingStmt = "CREATE TABLE " + dst_table_name + "\n(\n" + destTableStr + "\n);";
		System.out.println(createStagingStmt); //TODO: LOGGING
		
		PreparedStatement ps_CREATE_STAGING = c.prepareStatement(createStagingStmt);
		
		try {
			/** execute CREATE STAGING_TABLE sql */
			ps_CREATE_STAGING.execute();
		} catch (PSQLException e){ //TODO: make this generic for SQLException
	    	if ("42P07".equals(e.getSQLState())){
	    		System.out.println("ETL_LOGGER:" + e.getSQLState() + ", " + e.getMessage()); //TODO: LOGGING
	    	} else {
	    		throw abort("Uh-oh, something happened trying to build the ETL_STAGING_DB.", e); 
	    	}
	    } catch (Exception e) {
	    	throw abort("Uh-oh, something happened trying to build the ETL_STAGING_DB.", e); 
	    }

	    sqlaltertableAddColumn = addFlagColumn(dst_table_name);
	    PS_addcolumn_Flag = c.prepareStatement(sqlaltertableAddColumn);
	    System.out.println("ALTER STATEMENT: " + sqlaltertableAddColumn);
	    try {
	    	PS_addcolumn_Flag.execute();
	    } catch (PSQLException e){
	    	/** catch that column already exists. that is OK */
	    	if ("42701".equals(e.getSQLState())){
	    		System.out.println("ETL_LOGGER:" + e.getSQLState() + ", " + e.getMessage()); //TODO: LOGGING
	    	} else {
	    		throw abort("Uh-oh, something happened trying to add column etl_flag to ETL_STAGING_DB.", e); 
	    	}
	    } catch (Exception e) {
	    	throw abort("Uh-oh, something happened trying to add column etl_flag to ETL_STAGING_DB.", e); 
	    }
	    
		/** TODO: define this in a file. this is configurable */
		Properties props_mappings = new Properties();
		props_mappings.load(new FileInputStream("src-to-dst-column-mappings.properties"));
		Map<String, String> MAPPING_MAP = new LinkedHashMap<String,String>();
		Map<String, String> MAPPING_REV_MAP = new LinkedHashMap<String,String>();
		
		for (Entry<Object,Object> e : props_mappings.entrySet()){
			String key = ((String)e.getKey()).trim();
			String value = ((String)e.getValue()).trim();
			MAPPING_MAP.put(key, value);
			MAPPING_REV_MAP.put(value, key);
		}
/*		
		Map<String, String> MAPPING_MAP2 = new LinkedHashMap<String,String>(){{
			put("col_districtid","dis3");
			put("col_sex","sex");
			put("col_age","age");
			put("col_symp1","sx1");
			put("col_symp2","sx2");
			put("col_symp3","sx3");
			put("time","dteonset");
		}};
		Map<String, String> MAPPING_REV_MAP2 = new LinkedHashMap<String,String>(){{
			put("dis3","col_districtid");
			put("sex", "col_sex");
			put("age","col_age");
			put("sx1","col_symp1");
			put("sx2","col_symp2");
			put("sx3","col_symp3");
			put("dteonset","time");
		}};
		
*/		
		/** TODO build these guys programaticly when ETL_STAGING_DB is built with rs.metadata 
		 * 
		 * AFTER VERIFYING THE CODE DOWN LOWER TO COPY INTO FINAL, SAFE TO DELETE THIS
		 * */
/*		final String[] sex = {"sex","varchar"};
		final String[] dis = {"dis3","varchar"};
		final String[] age = {"age","int4"};
		final String[] sx1 = {"sx1","varchar"};
		final String[] sx2 = {"age","int4"};
		final String[] sx3 = {"age","int4"};
		final String[] dteonset = {"dteonset","timestamp"};
		Map<String, String[]> MEGA_MAP = new HashMap<String, String[]>(){{
			put("col_districtid",dis);
			put("col_sex", sex);
			put("col_age", age);
			put("col_symp1",sx1);
			put("col_symp2", sx2);
			put("col_symp3", sx3);
			put("time", dteonset);
//			put("col_test", new String[]{"dis3","varchar"});
		}};*/
		
		
		/** COPY FROM CLEANSEtable to STAGINGtable */
		PreparedStatement ps_SELECT_CLEANSING = c.prepareStatement("SELECT * FROM " + src_table_name);
		ResultSet rs_SELECT_CLEANSING = ps_SELECT_CLEANSING.executeQuery();
		
		
		PreparedStatement ps_SELECT_STAGING = c.prepareStatement("SELECT * FROM " + dst_table_name);
		ResultSet rs_SELECT_STAGING = ps_SELECT_STAGING.executeQuery();
		ResultSetMetaData rsmd = rs_SELECT_STAGING.getMetaData();

		//ResultSetMetaData rsmd = rs_SELECT_CLEANSING.getMetaData();
		String[] rsColsHERE = new String[rsmd.getColumnCount()];
		
	      int numberOfColumns = rsmd.getColumnCount();
	      for (int x=1; x < numberOfColumns ; x++){
	    	  System.out.println("LABEL: " + rsmd.getColumnLabel(x) + ", TYPE: " + rsmd.getColumnTypeName(x));
	    	  
	      }
	      for (Entry<String, String> dmap : DEST_COLTYPE_MAP.entrySet()){
	    	  System.out.println("LABEL: " + dmap.getKey() + ", TYPE: " + dmap.getValue());
	      }
	      
		for (int m=0; m <rsmd.getColumnCount(); m++){
			rsColsHERE[m] = rsmd.getColumnLabel(m+1);
		}

		// build the "INSERT INTO ETL_STAGING_DB" as reusable prepared statement
	    String stagingInsertStmt = "INSERT INTO " + dst_table_name; 
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
		
		ArrayList<String> alist = new ArrayList<String>(MAPPING_MAP.values());
		Map<String, Integer> REV_INDX_MAPPING_MAP = new HashMap<String, Integer>();
		int zIndx = -1;
		for (int z = 0; z<z_colCount; z++){
			String currentColName = rsmd2.getColumnLabel(z+1);
			System.out.println("currentColName: " + currentColName);
			if (MAPPING_MAP.get(currentColName) != null){
				String destColName = MAPPING_MAP.get(currentColName);
				System.out.println("destColName: " + destColName);
				zIndx = alist.indexOf(destColName);
				z_indexMap.put(destColName, new Integer(zIndx));
			}
		}
		
    	while (rs_SELECT_CLEANSING.next()){
//    		for (int z = 0; z<z_colCount; z++){
    		Set<Integer> masterindices_dst = new HashSet<Integer>(PARAMINDX_DST.values());
    		
    		for (Entry<String,Integer> z_indexEntry: z_indexMap.entrySet()){
    			//String currentColName = rs_SELECT_CLEANSING.
    			//if (!MAPPING_MAP.containsKey(currentColName)) continue;
    			String destColName = z_indexEntry.getKey();
    			String sourcColName = MAPPING_REV_MAP.get(destColName);
    			System.out.println("destcolNAME: "  + destColName);
    			Integer destIndx = z_indexEntry.getValue();
    			System.out.println("destINDX: " + destIndx); //TODO: verify this zIndex make sure it's right
    			Object VALUE = rs_SELECT_CLEANSING.getObject(sourcColName);
				System.out.println("THE VALUE AWAITED: "  + VALUE);
				
    			Integer SQL_TYPE = DEST_SQLTYPE_MAP.get(destColName);
    			if (SQL_TYPE == Types.DATE){
    				/** http://postgresql.1045698.n5.nabble.com/insert-from-a-select-td3279325.html */
    				System.out.println("date handling going on");
    				DateFormat formatter;
    				Date date;
    				String formatToUse = tocj.props_dateformats.getProperty(sourcColName).trim(); //i.e. "yyyy-MM-dd HH:mm:ss"
    				formatter = new SimpleDateFormat(formatToUse);//grab configured date format
    				try {
						date = (Date)formatter.parse(VALUE.toString());
						java.sql.Date sqlDate = new java.sql.Date(date.getTime());
						System.out.println(sqlDate.toString());
						ps_INSERT_STAGING.setDate(PARAMINDX_DST.get(destColName),sqlDate);
						System.out.println("SET THE DATE STUFF-" + sqlDate);
						masterindices_dst.remove(PARAMINDX_DST.get(destColName));
					} catch (ParseException e1) {
						// TODO Auto-generated catch block
						System.out.println("ERROR: Check your date pattern in the file dateformats.properties:\n\t" +
								sourcColName + "=" + tocj.props_dateformats.getProperty(sourcColName) +"\n");
						e1.printStackTrace();
						throw new Exception("Cannot proceed errors");
					}
    			} else {
//    				ps_INSERT_STAGING.setObject(destIndx+1, VALUE, SQL_TYPE);
    				ps_INSERT_STAGING.setObject(PARAMINDX_DST.get(destColName), VALUE, SQL_TYPE);
    				System.out.println("SET NON DATE-"+ VALUE );
    				masterindices_dst.remove(PARAMINDX_DST.get(destColName));
    			}
    		}
    		
    		/** set NULLS for parameters with no values */
    		for (Integer nullparamindx : masterindices_dst){
    			ps_INSERT_STAGING.setNull(nullparamindx, rsmd.getColumnType(nullparamindx));
    		}
    		//TODO: NEED THIS NOT HARDCODED TO 10--should be 1+ number of columns...
    		ps_INSERT_STAGING.setNull(PARAMINDX_DST.size() + 1, Types.VARCHAR);
    		ps_INSERT_STAGING.executeUpdate();
    	}
    	
    	System.out.println("FINISHED COMPLETE DONE");
    	/** TODO
    	 * 
    	 * !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    	 * INJECT THE CUSTOM SQL AGAINST THE STAGING TABLE HERE
    	 * !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    	 * 
    	 * */
    	
    	Properties customStagingSqlprops = tocj.props_customsql_staging;
		int numSql2 = customStagingSqlprops.size();
    	for (int i = 1; i <= numSql2; i++){
    		String sql = customStagingSqlprops.getProperty(String.valueOf(i));
    		sql = sql.replace("$table", dst_table_name);
    		System.out.println("CUSTOM SQL: " + sql);
    		PreparedStatement ps = c.prepareStatement(sql);
    		ps.execute();
    	}
    	
    	
    	
    	
    	/**MAPPING_MAP
		 * INSERT INTO "table_DEST" ("dst_column1", "dst_column2", ...)
			SELECT "src_column3", "src_column4", ...
			FROM "table_SRC"

		 */
    	// TODO THIS STUFF NEEDS TO BE REDONE IT IS THE COPY INTO THE FINAL DESTINATION TABLE...
    	// 
    	//
/**		String copysqlfront = "INSERT INTO ETL_STAGING_DB (";
		String copysqlmid = "SELECT ";
		String copysqlend = "FROM " + src_table_name;
		 //Set<Entry<String, String>> colmapz = MAPPING_MAP.entrySet();
		//int z=0;
		int z=0;
			
		for (Entry<String, String> e : MAPPING_MAP.entrySet()){
			String src_col = e.getKey();
			String dst_col = e.getValue();
			System.out.println("src_col:" + src_col);
			System.out.println("dst_col:" + dst_col);
//			copysqlfront += rsCols[z] + ",";
//			copysqlend += "?,";
			copysqlfront += dst_col + ",";
			copysqlmid += src_col + ",";
		}
		
		Set<String> src_columns = MAPPING_MAP.keySet();
		for (Iterator<String> colIterator = src_columns.iterator();colIterator.hasNext();){
			String src_col = colIterator.next();
		}
		
		
	    int lastC = copysqlfront.lastIndexOf(",");
	    copysqlfront = copysqlfront.substring(0, lastC);
	    copysqlfront += ")";
	    lastC = copysqlmid.lastIndexOf(",");
	    copysqlmid = copysqlmid.substring(0, lastC);
	    copysqlmid += " ";
	    
	    String copysql = copysqlfront + " " + copysqlmid + " " + copysqlend;
	    System.out.println("COPY FROM text to datatyped: " + copysql);
	    PreparedStatement copyStatement = c.prepareStatement(copysql);
	    z=0;
	    while (z<rsCols.length should be rs.next()){
	    	String targetColDataType = MEGA_MAP.get(rsCols[z])[1];
	    	int sqltype = 0;
	    	try {
				sqltype = EtlJdbcSupport.getTargetSqlType(targetColDataType, "postgres");
				System.out.println("the target sqltype: " + sqltype);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    	copyStatement.setObject(z, new Object(), sqltype);
	    	z++;
	    }*/
	    	
	    
//DESINTATION HANDLING	    
// take only the columns we care about in the "sourcecolumns.csv"
/**		CSVReader readerDest = new CSVReader(new FileReader("C:\\Documents and Settings\\POKUAM1\\workspace\\sandbox\\unittests\\datachunks\\destinationcolumns.csv"));
	    ArrayList<String[]> myEntriesDest = (ArrayList<String[]>) readerDest.readAll();
	    String[] headerDest = myEntriesDest.get(0);
	    // build CREATE TABLE
	    String table_nameDest = "ETL_DUMMY_DEST_TABLE";
	    String createStmtDest = "CREATE TABLE " + table_nameDest + " \n(";
	    for (String colHeadDest: headerDest){
	    	createStmtDest += colHeadDest + " varchar(255),\n";
	    }
	    int lastCommaDest = createStmtDest.lastIndexOf(",\n");
	    createStmtDest = createStmtDest.substring(0, lastCommaDest);
	    createStmtDest += "\n);";
	    
	    System.out.println("createstmtDest:\n" + createStmtDest);
	    
	    //LOAD FROM STAGING INTO DESTINATION
	    String mappingStr;
	    String[] mappings = myEntriesDest.get(1);
	    for (String mapping: mappings){
	    	//mappingStr += ""
	    }
	    // SELECT FROM ETL_STAGING_TABLE VALUES (colx, coly, colz, coln) INTO ETL_DUMMY_DEST_TABLE (dcolx, dcoly, dcolz, dcoln)
	    String transferStmt = "SELECT FROM " + src_table_name + " VALUES (";
	    //for (String mapping: )
	    	
	    	//http://dev.mysql.com/doc/refman/5.0/en/ansi-diff-select-into-table.html
	    	//http://www.postgresql.org/docs/8.1/static/sql-selectinto.html
		try {
			c.close();
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}*/ 	
	    
    	//File (or dir) to be moved
    	for (File file: readCsvFiles){
	    	//file = new File("filename");
	    	Date date = new Date();
	    	long dtime = date.getTime();
	    	//Destination dir
	    	File dir = new File(outputdir_csvfiles);
	    	
	    	//Move file to new dir
	    	boolean success = file.renameTo(new File(dir, dtime + "_"+ file.getName()));
	    	if (!success){
	    		//File not successfully moved
	    		//TODO: THIS DOESN'T RENAME IF FILE IS ALREADY THERE. MAYBE STICK ON TIMESTAMP TO MAKE UNIQUE??
	    		System.out.println("FILES NOT MOVED SOMETHING BAD HAPPENED. ROLLBACK EVERYTHING");
	    		//throw new Exception("FILES NOT MOVED SOMETHING BAD HAPPENED. ROLLBACK EVERYTHING");
	    	} else {
	    		file.delete();
	    	}
    	}
	}

	/**
	 * @return
	 */
	protected static String addFlagColumn(String tableToModify) {
	    String sqlaltertableAddColumn = "ALTER TABLE " + tableToModify + " ADD COLUMN etl_flag  varchar(255)";
		return sqlaltertableAddColumn;
	}
}
