package org.jhuapl.edu.sages.etl.oldstuff;
import java.io.File;
import java.io.FileInputStream;
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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.jhuapl.edu.sages.etl.ConnectionFactory;
import org.jhuapl.edu.sages.etl.ETLProperties;
import org.jhuapl.edu.sages.etl.SagesEtlException;
import org.postgresql.util.PSQLException;

import au.com.bytecode.opencsv.CSVReader;

/**
 * @author POKUAM1
 * @created Oct 4, 2011
 */
public class TestOpenCsvJar {

	private File[] csvFiles;
	private File currentFile;
	private File fileMarkedForDeletion;
	private static ArrayList<String[]> currentEntries;
	private static int currentRecNum;
	//private static List<File> failedCsvFiles;
	private static boolean success;
	
	
	/** csv files are loaded from inputdir and moved to outputdir after being processed successfully  */
	private static String inputdir_csvfiles;
	private static String outputdir_csvfiles;
	private static String faileddir_csvfiles;
	
	private static final String ETL_CLEANSE_TABLE = "ETL_CLEANSE_TABLE";
	private static final String ETL_STAGING_DB = "ETL_STAGING_DB";
	private static String src_table_name;
	private static String dst_table_name;
	private static String prod_table_name;
	/** maps the destination columns to their sql-datatype qualifier for generating the schema */
	private static Map<String,String> DEST_COLTYPE_MAP;
	
	/** maps the destination columns to their java.sql.Types for setting ? parameters on prepared statements */
	//http://download.oracle.com/javase/6/docs/api/constant-values.html#java.sql.Types.TIME
	private static Map<String, Integer> DEST_SQLTYPE_MAP;
	
	/** maps the source:destination columns*/
	private static Map<String, String> MAPPING_MAP;
	/** maps the destination:source columns*/
	private static Map<String, String> MAPPING_REV_MAP;
		
	/** properties holders */
	private Properties props_etlconfig;
	private Properties props_mappings;
	private Properties props_dateformats;
	private Properties props_customsql_cleanse;
	private Properties props_customsql_staging;
	private Properties props_customsql_final_to_prod;
		
	/** target database connection settings*/
	private String dbms;
	private int portNumber;
	private String serverName;
	private String dbName;
	private String userName;
	private String password;
		
	/** maps source column name to its parameter index in the source table, indexing starts at 1 */
	private static Map<String,Integer> PARAMINDX_SRC = new HashMap<String,Integer>();
	/** maps destination column name to its parameter index in the destination table, indexing starts at 1 */
	private static Map<String,Integer> PARAMINDX_DST = new HashMap<String,Integer>();
		
	/** header columns used to define the CLEANSE table schema */
	private static String[] header_src = new String[0];
	
	/** errorFlag to control what to do on certain errors */
	private static int errorFlag = 0;
	/**
	 * @param dbms
	 * @param portNumber
	 * @param userName
	 * @param password
	 * @param serverName
	 * @param dbName
	 * @throws SagesEtlException 
	 */
	public TestOpenCsvJar(String dbms, int portNumber, String userName,
			String password, String serverName, String dbName) throws SagesEtlException {
		super();
		ETLProperties etlProperties = new ETLProperties();
		etlProperties.loadEtlProperties();
		initializeProperties(etlProperties);

		//override db connection settings
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
		ETLProperties etlProperties = new ETLProperties();
		etlProperties.loadEtlProperties();
		initializeProperties(etlProperties);
	}

	/**
	 * @throws SagesEtlException
	 */
	protected void initializeProperties(ETLProperties etlProperties) throws SagesEtlException {
		this.props_etlconfig = etlProperties.getProps_etlconfig();
		this.props_mappings = etlProperties.getProps_mappings();
		this.props_dateformats = etlProperties.getProps_dateformats();
		this.props_customsql_cleanse = etlProperties.getProps_customsql_cleanse();
		this.props_customsql_staging = etlProperties.getProps_customsql_staging();
		this.props_customsql_final_to_prod = etlProperties.getProps_customsql_final_to_prod();
		this.dbms = etlProperties.getDbms();
		this.portNumber = etlProperties.getPortNumber();
		this.userName = etlProperties.getUserName();
		this.password = etlProperties.getPassword();
		this.serverName = etlProperties.getServerName();
		this.dbName = etlProperties.getDbName();
		
		this.inputdir_csvfiles = props_etlconfig.getProperty("csvinputdir");
		this.outputdir_csvfiles = props_etlconfig.getProperty("csvoutputdir");
		this.faileddir_csvfiles = props_etlconfig.getProperty("csvfaileddir");
	}
		
	/**
	 * Establishes database connection to the target database
	 * @return Connection
	 * @throws SQLException
	 */
	public Connection getConnection() throws SQLException {
		return ConnectionFactory.createConnection(this.dbms, this.serverName, this.dbName,
										   this.userName, this.password, this.portNumber);
    }

	/**
	 * returns a SagesEtlException that wraps the original exception	
	 * @param msg SAGES ETL message to display
	 * @param e the original exception
	 * @return SagesEtlException
	 */
	public static SagesEtlException abort(String msg, Throwable e){
		return new SagesEtlException(e.getMessage(), e);
	} 
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		Connection c = null;
		TestOpenCsvJar tocj = null;
		//File f = new File("csvfilerepo\\in\\del_formdata_oevisit11106-1715.csv");
		//System.out.println("file deletable?" + f.canWrite());
		//FileUtils.forceDelete(f);
		//System.out.println("file exists?" + f.exists());
		
		
//	    FileUtils fu = new FileUtils();
//    	//File (or dir) to be moved
//	    //for (File file: readCsvFiles){
//    	for (File file: csvFiles){
//	    	Date date = new Date();
//	    	long dtime = date.getTime();
//	    	/** Destination dir */
//	    	File dir = new File(outputdir_csvfiles);
//	    	/** Move file to new dir */
//	    	fu.copyFile(file, new File(dir, dtime + "_"+ file.getName()));
//	    	//boolean success = file.renameTo(new File(dir, dtime + "_"+ file.getName()));
//	    	Thread.sleep(2000);
//	    	fu.forceDelete(file);
//	    	c.commit();
//	    	System.out.println("SAGES ETL PROCESS FINISHED SUCCESS.");
//	    	if (false){
//	    		//File not successfully moved
//	    		System.out.println("FILES NOT MOVED SOMETHING BAD HAPPENED. WAIT 2 SECONDS. THEN LOOP RETRY");
//	    		Thread.sleep(2000);
//	    		//TODO: THIS DOESN'T RENAME IF FILE IS ALREADY THERE. MAYBE STICK ON TIMESTAMP TO MAKE UNIQUE??
//	    		//throw new Exception("FILES NOT MOVED SOMETHING BAD HAPPENED. ROLLBACK EVERYTHING");
//	    			Thread.sleep(10000);
//	    			System.out.println("FILE STILL NOT MOVED SOMETHING BAD HAPPENED. WAIT 10 SECONDS. THEN LOOP RETRY");
//	    	} else {
//	    		file.delete();
//	    	}
//    	}
		ArrayList<File> files2delete = new ArrayList<File>();
		try {
			tocj = new TestOpenCsvJar();
			
			String path = tocj.outputdir_csvfiles;
			File dir1 = new File(path);
			File[] files1 = dir1.listFiles();
			
/*			for (File file: dir1.listFiles()){
				tocj.fileMarkedForDeletion = file;
				File tmpFile = new File(file.getPath());
				FileUtils.forceDelete(tmpFile);
			}*/
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
		} catch (SagesEtlException e){
			throw abort("Unable to load properties",e );
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
//		String inputdir_csvfiles = tocj.props_etlconfig.getProperty("csvinputdir");
//		String outputdir_csvfiles = tocj.props_etlconfig.getProperty("csvoutputdir");
//		String faileddir_csvfiles = tocj.props_etlconfig.getProperty("csvfaileddir");
		
		ArrayList<String[]> master_entries_rawdata = new ArrayList<String[]>();

		extractHeaderColumns(tocj);
		
		
		/** load into memory all csv files in the inputdir
		 *  - each iteration is new transaction so we can isolate erroneous files 
		 **/
//		int f = 0; /** to grab the header from the first csv file */
		File deletethismess = null;
		for (File file : tocj.csvFiles){
			success = false;
			master_entries_rawdata.clear();
//notused			tocj.currentFile = file; //TODO currentEntries, currentRecNum
			
/*			if (tocj.fileMarkedForDeletion != null) {
				FileUtils.forceDelete(tocj.fileMarkedForDeletion);
				c.commit();
				tocj.fileMarkedForDeletion = null;
			}*/
			
			CSVReader reader_rawdata2 = new CSVReader(new FileReader(file));
			currentEntries = (ArrayList<String[]>) reader_rawdata2.readAll();
			currentEntries.remove(0);  /** remove the header row */
			master_entries_rawdata.addAll(currentEntries);
		
			ArrayList<String[]> entries_rawdata = master_entries_rawdata;
	    
	    /*********************************** 
	     * SAVEPOINT #1 
	     ***********************************
	     * before CLEANSE & STAGING built 
	     ***********************************/
	     Savepoint save1 = c.setSavepoint("save1");
	    
	    int lastComma;
		Savepoint createCleanseSavepoint = buildCleanseTable(c, tocj, save1); 
	    
		String sqlaltertableAddColumn;
		PreparedStatement PS_addcolumn_Flag;
		alterCleanseTableAddFlagColumn(c, save1, createCleanseSavepoint);

    
	    int lastTick; //TODO shouldn't this create a new savepoint: createStagingSavepoint???
		buildStagingTable(c, tocj, save1);

	    alterStagingTableAddFlagColumn(c, save1, createCleanseSavepoint);
	    
	    
		/** TODO: define this in a file. this is configurable */
		//Properties props_mappings = new Properties();
		//props_mappings.load(new FileInputStream("src-to-dst-column-mappings.properties"));
		generateSourceDestMappings(tocj);
	    
	    
	    /***************************************** 
	     * SAVEPOINT #2 
	     *****************************************
	     * before INSERT csv records into CLEANSE
	     *****************************************/
	     Savepoint save2 = c.setSavepoint("save2");
	    
	    PreparedStatement ps_INSERT_CLEANSE = c.prepareStatement(buildInsertIntoCleansingTableSql(c,tocj));
	    	
    	setAndExecuteInsertIntoCleansingTablePreparedStatement(c, tocj,
				entries_rawdata, save2, ps_INSERT_CLEANSE);
	    
	    /** TODO
	     * 
	     * !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	     * INJECT THE CUSTOM SQL AGAINST THE CLEANSE TABLE HERE
	     * !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	     * 
	     * */
	    try {
	    	Properties customCleanseSqlprops = tocj.props_customsql_cleanse;
	    	int numSql = customCleanseSqlprops.size();
	    	for (int i = 1; i <= numSql; i++){
	    		String sql = customCleanseSqlprops.getProperty(String.valueOf(i));
	    		sql = sql.replace("$table", src_table_name);
	    		System.out.println("CUSTOM SQL: " + sql);
	    		PreparedStatement ps = c.prepareStatement(sql);
	    		ps.execute();
	    	}
	    } catch (Exception e){
			errorCleanup(save2, c, null, e);
			System.out.println("ETL_LOGGER: failure with custom sql against staging.");
		}
	    


		/** COPY FROM CLEANSEtable to STAGINGtable */
		copyFromCleanseToStaging(c, tocj, save2);
		
    	System.out.println("FINISHED COMPLETE DONE");
    	/** TODO
    	 * 
    	 * !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    	 * INJECT THE CUSTOM SQL AGAINST THE STAGING TABLE HERE
    	 * !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    	 * 
    	 * */

    	try {
	    	Properties customStagingSqlprops = tocj.props_customsql_staging;
			int numSql2 = customStagingSqlprops.size();
	    	for (int i = 1; i <= numSql2; i++){
	    		String sql = customStagingSqlprops.getProperty(String.valueOf(i));
	    		sql = sql.replace("$table", dst_table_name);
	    		System.out.println("CUSTOM SQL: " + sql);
	    		PreparedStatement ps = c.prepareStatement(sql);
	    		ps.execute();
	    	}
    	} catch (Exception e){
			errorCleanup(save2, c, null, e);
			System.out.println("ETL_LOGGER: failure with custom sql against staging.");
		}
   
    	
    	
    	System.out.println("FINAL LOAD INTO THE PRODUCTION TABLE STARTING.");
    	/** TODO
    	 * 
    	 * !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    	 * INJECT THE CUSTOM TRANSFER SQL AGAINST THE FINAL PRODUCTION TABLE HERE
    	 * !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    	 * 
    	 * */
    	//INSERT INTO $staging_table ($columnlist) SELECT $prodcolumnlist FROM $staging_table
		prod_table_name = tocj.props_etlconfig.getProperty("tableNamePattern");
    	String insertSelect_Production = "INSERT INTO " + prod_table_name + " (_columnlist_) SELECT _columnlist_ FROM " + dst_table_name;
    	
    	//TODO: don't want to build this each time. enhance
    	String columnlist = StringUtils.join(DEST_COLTYPE_MAP.keySet(), ","); 
    	System.out.println("ETL_LOGGER(columnList): " + columnlist);

    	insertSelect_Production = insertSelect_Production.replaceAll("_columnlist_", columnlist);
    	System.out.println("ETL_LOGGER(insertSelect_Production): " + insertSelect_Production);
    	
    	try {
    		PreparedStatement ps_finalToProd = c.prepareStatement(insertSelect_Production);
    		ps_finalToProd.execute();
    	} catch (Exception e) {
    		System.out.println("ETL_LOGGER: OOOOOOPS SOMETHING HAPPENED BAD IN THE END. SHUCKS");
    		errorCleanup(save2, c, null, e);
    	}
    	
    	try {
	    	Properties customProdLoaderSqlprops = tocj.props_customsql_final_to_prod;
			int numSql3 = customProdLoaderSqlprops.size();
	    	for (int i = 1; i <= numSql3; i++){
	    		String sql = customProdLoaderSqlprops.getProperty(String.valueOf(i));
	    		sql = sql.replace("$table", dst_table_name);
	    		System.out.println("CUSTOM SQL: " + sql);
	    		PreparedStatement ps = c.prepareStatement(sql);
	    		ps.execute();
	    	}
    	} catch (Exception e){
			errorCleanup(save2, c, file, e);
			System.out.println("ETL_LOGGER: failure with custom sql final to prod: " + e.getMessage());
		}
    	
    	if (!success && errorFlag == 9){
    		// MOVE THIS FILE TO FAILED
    	} else {
    		try{
    			//tocj.fileMarkedForDeletion = file;
		    	Date date = new Date();
		    	long dtime = date.getTime();
		    	/** Destination dir */
		    	File dir = new File(outputdir_csvfiles);
		    	/** Move file to new dir */
		    	FileUtils.copyFile(file, new File(dir, dtime + "_"+ file.getName()));
		    	//boolean success = file.renameTo(new File(dir, dtime + "_"+ file.getName()));
		    	Thread.sleep(2000);
		    	//deletethismess = new File(file.getPath());
		    	//FileUtils.forceDelete(deletethismess);
		    	FileUtils.forceDelete(file);
		    	//c.commit();
    		} catch (IOException io){
    			System.out.println("CRAP THAT'S NOT GOOD COULDNT MOVE/DELETE FILE TO OUTPUT: " + io.getMessage());
    		}
    	}
	}
	
		c.commit();

	}

	/**
	 * @param c
	 * @param tocj
	 * @param save2
	 * @throws SQLException
	 */
	protected static void copyFromCleanseToStaging(Connection c,
			TestOpenCsvJar tocj, Savepoint save2) throws SQLException {
		int lastComma;
		int lastTick;
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
		
		try {
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
							errorCleanup(save2, c, null, e1);
							throw new SagesEtlException("Cannot proceed errors");
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
		} catch (Exception e){
			errorCleanup(save2, c, null, e);
			System.out.println("ETL_LOGGER: error did occur for this file, but rolled back and proceeding.");
		}
	}

	/**
	 * @param c
	 * @param tocj
	 * @param entries_rawdata
	 * @param save2
	 * @param ps_INSERT_CLEANSE
	 * @throws SQLException
	 */
	protected static void setAndExecuteInsertIntoCleansingTablePreparedStatement(
			Connection c, TestOpenCsvJar tocj,
			ArrayList<String[]> entries_rawdata, Savepoint save2,
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
	    	if (tocj.dbms.equals(ETLProperties.dbid_msaccess)){
	    		ps_INSERT_CLEANSE.setString(entry.length + 1, "no flag");
	    		log_insertStmt += "'no flag'"; 
	    	}
	    	
	    	System.out.println("ETL_LOGGER:(ps_INSERT_CLEANSE)= " + ps_INSERT_CLEANSE.toString());
	    	System.out.println("ETL_LOGGER: " + log_insertStmt); //TODO: LOGGING
	    	try {
	    		ps_INSERT_CLEANSE.execute();
	    	} catch (Exception e1){
	    		if(errorCleanup(save2, c, null, e1) == 2){
	    			break;
	    		}
	    	}
	    }
	}

	/**
	 * @param c
	 * @param tocj
	 * @return
	 * @throws SQLException
	 */
	protected static String buildInsertIntoCleansingTableSql(
			Connection c, TestOpenCsvJar tocj) throws SQLException {
	    /************************************************ 
	     * build reusable 'INSERT INTO CLEANSING_TABLE'
	     ************************************************
	     * SQL: "INSERT INTO SRC_TABLE..."
	     * Example SQL: "INSERT INTO src_table_name VALUES (?, ?, ?, ?, ?,...)"
	     * data will be inserted as text sql-datatype
	     *  
	     */

		int lastTick;
		String insertStmt_src = "INSERT INTO " + src_table_name + " VALUES (";
	    
	    for (int h=0; h < header_src.length; h++){
	    	insertStmt_src = insertStmt_src + "?,"; 
	    }
	    
    	/***
    	 * MS Access specific 
    	 * 2351 - Microsoft Access can't represent an implicit VALUES clause in the query design grid. 
    	 * Edit this in SQL view.
    	 * 
    	 * (this is for the "etl_flag" column that was added after table creation
    	 */
    	if (tocj.dbms.equals(ETLProperties.dbid_msaccess)){
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
	 * @param tocj
	 */
	protected static void generateSourceDestMappings(TestOpenCsvJar tocj) {
		MAPPING_MAP = new LinkedHashMap<String,String>();
		MAPPING_REV_MAP = new LinkedHashMap<String,String>();
		
		for (Entry<Object,Object> e : tocj.props_mappings.entrySet()){
			String key = ((String)e.getKey()).trim();
			String value = ((String)e.getValue()).trim();
			MAPPING_MAP.put(key, value);
			MAPPING_REV_MAP.put(value, key);
		}
	}

	/**
	 * @param c
	 * @param save1
	 * @param createCleanseSavepoint
	 * @throws SQLException
	 * @throws SagesEtlException
	 */
	protected static void alterStagingTableAddFlagColumn(Connection c,
			Savepoint save1, Savepoint createCleanseSavepoint)
			throws SQLException, SagesEtlException {
		String sqlaltertableAddColumn;
		PreparedStatement PS_addcolumn_Flag;
		sqlaltertableAddColumn = addFlagColumn(dst_table_name);
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
	    		errorCleanup(save1, c, null, e);
	    		throw abort("Uh-oh, something happened trying to add column etl_flag to ETL_STAGING_DB.", e); 
	    	}
	    } catch (SQLException e){ //TODO MS Access specific
	    	if ("S0021".equals(e.getSQLState())){
	    		System.out.println("ETL_LOGGER:" + e.getSQLState() + ", " + e.getMessage()); //TODO: LOGGING
	    	} else {
	    		errorCleanup(save1, c, null, e);
	    		throw abort("Uh-oh, something happened trying to add column etl_flag to ETL_CLEANSING_TABLE.", e); 
	    	}
	    } catch (Exception e) {
	    	errorCleanup(save1, c, null, e);
	    	throw abort("Uh-oh, something happened trying to add column etl_flag to ETL_STAGING_DB.", e); 
	    }
	}

	/**
	 * @param c
	 * @param tocj
	 * @param save1
	 * @throws SQLException
	 * @throws SagesEtlException
	 */
	protected static void buildStagingTable(Connection c, TestOpenCsvJar tocj,
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
		String tableNamePattern = tocj.props_etlconfig.getProperty("tableNamePattern"); //TODO rename this
		String columnNamePattern = null;
		
		ResultSet rs_FINAL = dbmd.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);

		String destTableStr = "";
		dst_table_name = tocj.props_etlconfig.getProperty("dbprefix_dst") + "_" + ETL_STAGING_DB;
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
			DEST_COLTYPE_MAP.put(colName, colType);
			DEST_SQLTYPE_MAP.put(colName, colSqlType);
	    	PARAMINDX_DST.put(colName, zdst);
	    	zdst++;
		}
		
		/** the built "CREATE TABLE STAGING_TABLE..." string */
	    Savepoint createStagingSavepoint = c.setSavepoint("createStagingSavePoint");
		/** remove trailing ','  TODO CLEAN UP WITH StringUtils.join()  */
	    int lastTick = destTableStr.lastIndexOf(",");
	    destTableStr = destTableStr.substring(0, lastTick);
	    String createStagingStmt = "CREATE TABLE " + dst_table_name + "\n(\n" + destTableStr + "\n);";
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
	    		errorCleanup(save1, c, null, null);
	    		throw abort("Uh-oh, something happened trying to build the ETL_STAGING_DB.", e); 
	    	}
	    } catch (SQLException e){ //TODO: make this generic for SQLException this is MS Access error
	    	if ("S0001".equals(e.getSQLState())){
	    		System.out.println("ETL_LOGGER:" + e.getSQLState() + ", " + e.getMessage()); //TODO: LOGGING
	    	} else {
	    		errorCleanup(save1, c, null, null);
	    		throw abort("Uh-oh, something happened trying to build the ETL_CLEANSING_TABLE.", e); 
	    	}
	    } catch (Exception e) {
	    	errorCleanup(save1, c, null, null);
	    	throw abort("Uh-oh, something happened trying to build the ETL_STAGING_DB.", e); 
	    }
	}

	/**
	 * @param c
	 * @param save1
	 * @param createCleanseSavepoint
	 * @throws SQLException
	 * @throws SagesEtlException
	 */
	protected static void alterCleanseTableAddFlagColumn(Connection c,
			Savepoint save1, Savepoint createCleanseSavepoint)
			throws SQLException, SagesEtlException {
		String sqlaltertableAddColumn = addFlagColumn(src_table_name);
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
	    		errorCleanup(save1, c, null, e);
	    		throw abort("Uh-oh, something happened trying to add column etl_flag to ETL_CLEANSING_TABLE.", e); 
	    	}
	    } catch (SQLException e){ //TODO MS Access specific
	    	if ("S0021".equals(e.getSQLState())){
	    		System.out.println("ETL_LOGGER:" + e.getSQLState() + ", " + e.getMessage()); //TODO: LOGGING
	    	} else {
	    		errorCleanup(save1, c, null, e);
	    		throw abort("Uh-oh, something happened trying to add column etl_flag to ETL_CLEANSING_TABLE.", e); 
	    	}
	    } catch (Exception e) {
	    	errorCleanup(save1, c, null, e);
	    	throw abort("Uh-oh, something happened trying to add column etl_flag to ETL_CLEANSING_TABLE.", e); 
	    }
	}

	/**
	 * @param c
	 * @param tocj
	 * @param save1
	 * @return
	 * @throws SQLException
	 * @throws SagesEtlException
	 */
	protected static Savepoint buildCleanseTable(Connection c,
			TestOpenCsvJar tocj, Savepoint save1) throws SQLException,
			SagesEtlException {
		/*********************************** 
	     * build ETL_CLEANSE_TABLE 
	     ***********************************
	     * SQL: "CREATE TABLE..." 
	     * - all columns have text sql-datatype
	     * - column definitions built from csv file header 
	     ****************************/
	     //http://postgresql.1045698.n5.nabble.com/25P02-current-transaction-is-aborted-commands-ignored-until-end-of-transaction-block-td2174290.html
	    src_table_name = tocj.props_etlconfig.getProperty("dbprefix_src") + "_" + ETL_CLEANSE_TABLE;
	    String createStmt_src = "CREATE TABLE " + src_table_name + " \n(\n";
	    
	    /**
	     * build schema definition & map column to its index for later use with ResultSet and PreparedStatment
	     */
	    int zsrc=1;
	    for (String colHead_src: header_src){
	    	if (tocj.dbms.equals(ETLProperties.dbid_msaccess) && "time".equals(colHead_src)){
	    		colHead_src = "accessetl_time";
	    	}
	    	createStmt_src += colHead_src + " varchar(255),\n";
	    	PARAMINDX_SRC.put(colHead_src, zsrc);
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
	    		errorCleanup(save1, c, null, e);
	    		throw abort("Uh-oh, something happened trying to build the ETL_CLEANSING_TABLE.", e); 
	    	}
	    } catch (SQLException e){ //TODO: make this generic for SQLException this is MS Access error
	    	if ("S0001".equals(e.getSQLState())){
	    		System.out.println("ETL_LOGGER:" + e.getSQLState() + ", " + e.getMessage()); //TODO: LOGGING
	    	} else {
	    		errorCleanup(save1, c, null, e);
	    		throw abort("Uh-oh, something happened trying to build the ETL_CLEANSING_TABLE.", e); 
	    	}
	    } catch (Exception e) {
	    	errorCleanup(save1, c, null, e);
	    	throw abort("Uh-oh, something happened trying to build the ETL_CLEANSING_TABLE.", e); 
	    }
		return createCleanseSavepoint;
	}

	/**
	 * @param tocj
	 * @return
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	protected static void extractHeaderColumns(TestOpenCsvJar tocj)
			throws FileNotFoundException, IOException {
		/** header columns used to define the CLEANSE table schema */
		header_src = new String[0];
		//failedCsvFiles = new ArrayList<File>();
		//File[] readCsvFiles = new File[0];
		
		/** determine header columns **/
		File csvinputDir = new File(inputdir_csvfiles);
		if (csvinputDir.isDirectory()){
			tocj.csvFiles = csvinputDir.listFiles();
			if (tocj.csvFiles.length == 0){
				System.out.println("Nothing to load into database. Exiting."); //TODO: LOGGING explain this happens
				System.exit(0); 
			} 
			header_src = determineHeaderColumns(tocj.csvFiles[0]);
		} else {
			System.out.println(csvinputDir.getName() + "is not valid csv input directory. Exiting."); //TODO: LOGGING explain this happens
			System.exit(0); 	
		}
		//return readCsvFiles;
	}

	/**
	 * @param master_entries_rawdata
	 * @param file
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	protected static String[] determineHeaderColumns(File file) throws FileNotFoundException, IOException {
		CSVReader reader_rawdata2 = new CSVReader(new FileReader(file));
		ArrayList<String[]> currentEntries = (ArrayList<String[]>) reader_rawdata2.readAll();
		String[] headerColumns = currentEntries.get(0); /** set header from the first csv file */
		//currentEntries.remove(0); /** remove the header row */
		//master_entries_rawdata.addAll(currentEntries);
		return headerColumns;
	}

	protected static int errorCleanup(Savepoint savepoint, Connection connection, File currentCsv, Exception e){
		String savepointName = "";
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
		    	/** Destination dir */
		    	File dir = new File(faileddir_csvfiles);
		    	/** Move file to new dir */
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

	/**
	 * @return
	 */
	protected static String addFlagColumn(String tableToModify) {
	    String sqlaltertableAddColumn = "ALTER TABLE " + tableToModify + " ADD COLUMN etl_flag  varchar(255)";
		return sqlaltertableAddColumn;
	}
}
