/**
 * 
 */
package org.jhuapl.edu.sages.etl.strategy;

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

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.jhuapl.edu.sages.etl.ETLProperties;
import org.jhuapl.edu.sages.etl.SagesEtlException;

/**
 * {@link ETLPostgresqlStrategy} is the Postgresql specific strategy for the ETL processing logic. 
 * Transaction handling, SQL syntax nuances, and error codes are for Postgresql.
 * @author POKUAM1
 * @created Nov 1, 2011
 */
public class ETLPostgresqlStrategy extends ETLStrategyTemplate {
	private static final Logger log = Logger.getLogger(ETLPostgresqlStrategy.class);
	
	private static final String msgFatal = "ETL_LOGGER: error did occur for this file, but data is rolled back to last good state.";
	public static final Set<String> postgresqlIgnorableErrorCodes = new HashSet<String>(){{
		
		// "42P07" => relation "oevisit_etl_cleanse_table" already exists
		// "42P07" => relation "oevisit_etl_staging_db" already exists
		add("42P07");
		
		//add("25P02"); // column "etl_flag" of relation "oevisit_etl_staging_db" already exists
		
		// "42701" => column "etl_flag" of relation "oevisit_etl_cleanse_table" already exists
		// "42701" => column "etl_flag" of relation "oevisit_etl_staging_db" already exists
		add("42701"); 

		// "42P01" => relation "oevisit_etl_cleanse_table" does not exist
		// "42P01" => relation "oevisit_etl_staging_db" does not exist
		add("42P01");
		add("CODE");
		add("CODE");
		add("CODE");
	}};
	
	public ETLPostgresqlStrategy(SagesOpenCsvJar socj){
		super();
		m_socj = socj;
		super.m_sqlStateHandler.setIgnorableErrorCodes(postgresqlIgnorableErrorCodes);
	}

	public ETLPostgresqlStrategy(){
		super.m_sqlStateHandler.setIgnorableErrorCodes(postgresqlIgnorableErrorCodes);
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
		} catch (Exception e){
			m_sqlStateHandler.sqlExceptionHandlerBuildStagingTable(c, socj, save1, createStagingSavepoint, e);
		}
		return createStagingSavepoint; 
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
	    		if(m_sqlStateHandler.errorCleanup(m_socj, save2, c, socj.getCurrentFile(), socj.getFaileddir_csvfiles(), e1) == 2){
	    			break;
	    		}
	    	}
	    }
	}

	@Override
	public void copyFromCleanseToStaging(Connection c, SagesOpenCsvJar socj, Savepoint save2) throws SQLException, SagesEtlException {
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
		
		int recNum = 0;
		try {
	    	while (rs_SELECT_CLEANSING.next()){
	//    		for (int z = 0; z<z_colCount; z++){
	    		recNum++;
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
	    			if (SQL_TYPE == null){
    					log.fatal("'" + destColName + "' does not exist as a destination column in the production table, but was used in the src-to-dst-column-mappings.properties file. Check the mapping file for mistakes.");
    					throw SagesOpenCsvJar.abort("'" + destColName + "' does not exist as a destination column in the production table, but was used in the src-to-dst-column-mappings.properties file. Check the mapping file for mistakes." + destColName, new NullPointerException());
	    				
	    			}
	    			if (VALUE.equals("")){
	    				VALUE = null;
	       				ps_INSERT_STAGING.setObject(socj.PARAMINDX_DST.get(destColName), VALUE, SQL_TYPE);
	    				log.debug("SET NON DATE-"+ VALUE );
	    				masterindices_dst.remove(socj.PARAMINDX_DST.get(destColName));
	    			}
	    			
	    			else if (SQL_TYPE == Types.DATE){
	    				/** http://postgresql.1045698.n5.nabble.com/insert-from-a-select-td3279325.html */
	    				log.debug("date handling now occurring");
	    				DateFormat formatter;
	    				Date date = null;
	    				java.sql.Date sqlDate = null;
	    				String formatToUse = socj.props_dateformats.getProperty(sourcColName); //i.e. "yyyy-MM-dd HH:mm:ss", "dd.MM.yyyy"
	    				if (formatToUse == null){
	    					log.fatal("Date formatter was defined for a column '" + sourcColName + "' that does not exist in the .csv input files. Check dateformats.properties");
	    					throw SagesOpenCsvJar.abort("Date formatter was defined for a column '" + sourcColName + "' that does not exist in the .csv input files: " + sourcColName, new NullPointerException());
	    				} else {
	    					formatToUse.trim();
	    				}
	    				formatter = new SimpleDateFormat(formatToUse);//grab configured date format
	    				try {
	    					if (VALUE.equals("")){ //"" => null date handling
	    						
	    					}else{
	    						date = (Date)formatter.parse(VALUE.toString());
	    						sqlDate = new java.sql.Date(date.getTime());
	    						log.debug(sqlDate.toString());
	    					}
							ps_INSERT_STAGING.setDate(socj.PARAMINDX_DST.get(destColName),sqlDate);
							log.debug("SET THE DATE STUFF-" + sqlDate);
							masterindices_dst.remove(socj.PARAMINDX_DST.get(destColName));
						} catch (ParseException e1) {
							// TODO Auto-generated catch block
							log.fatal("ERROR: Check your date pattern in the file dateformats.properties:\n\t" +
									sourcColName + "=" + socj.props_dateformats.getProperty(sourcColName) +"\n");
							e1.printStackTrace();
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

	    		/** THIS IS FOR THE COLUMN ETL_FLAG, which is at (1 + totalCols),
	    		 *  it gets set to NULL in the STAGING TABLE **/
	    		ps_INSERT_STAGING.setNull(socj.PARAMINDX_DST.size() + 1, Types.VARCHAR);
	    		ps_INSERT_STAGING.executeUpdate();
	    	}
		} catch(SQLException se){
			log.fatal(msgFatal);
			log.fatal("Line " + recNum + " caused an error.");
			throw  SagesOpenCsvJar.abort(se.getMessage(), se);
		} catch (Exception e){
			log.fatal(msgFatal);
			log.fatal("Line " + recNum + " caused an error.");
			throw  SagesOpenCsvJar.abort(e.getMessage(), e);
		}
	}

}
