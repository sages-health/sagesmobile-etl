package org.jhuapl.edu.sages.etl.opencsvpods;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.jhuapl.edu.sages.etl.SagesEtlException;
import org.jhuapl.edu.sages.etl.strategy.ETLPostgresqlStrategy;
import org.jhuapl.edu.sages.etl.strategy.ETLStrategy;
import org.jhuapl.edu.sages.etl.strategy.SagesOpenCsvJar;

import au.com.bytecode.opencsv.CSVReader;

/**
 * @author POKUAM1
 * @created Oct 4, 2011
 */
public class DumbTestOpenCsvJar extends SagesOpenCsvJar {

	/**
	 * @throws SagesEtlException
	 */
	public DumbTestOpenCsvJar() throws SagesEtlException {
		super();
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param args
	 * @throws IOException 
	 * @throws Exception 
	 */
	public static void main(String[] args) throws SagesEtlException, IOException {
		/**
		 1. LOAD PROPERTIES
		 2. ESTABLISH DB CONNECTION
		 3. TOGGLE PROPS: TRANSACTION, LOGGING, STATS, ETC...
		 4. SET ETLSTRATEGY (*dpattern)
		 5. LOAD CSV FILES
		 6. ETL per CSV FILE 
		      (on success: copy out, delete from in ) COMMIT
		      (on failure: copy fail, delete from in) ROLLBACK
		 */

		
		/** LOAD PROPERTIES *************************************************************/
		DumbTestOpenCsvJar socj_dumb = new DumbTestOpenCsvJar();
		
		/** ESTABLISH DB CONNECTION *****************************************************/
		Connection c = null;
		c = socj_dumb.getConnection();

		/** SET EtlStrategy *************************************************************/
		socj_dumb.setEtlStrategy(new ETLPostgresqlStrategy(socj_dumb));
		
		ArrayList<String[]> master_entries_rawdata = new ArrayList<String[]>();
		
		try {
			socj_dumb.extractHeaderColumns(socj_dumb);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		String DEBUGheader_src = StringUtils.join(socj_dumb.header_src);
		System.out.println("header_src:\n" + DEBUGheader_src);
		
		/** LOAD CSV FILES ***************************************************************
		 *  - load into memory all csv files in the inputdir
		 *  - each iteration is new transaction so we can isolate erroneous files 
		 */
		//build CLEANSE
		//alter CLEANSE
		//build STAGING
		//alter STAGING
		//build MAPPINGS
		//insert into CLEANSE
		//select insert into STAGING
		//select insert into FINAL PRODUCTION
		
		//--CLEANUP--
		//copy file to OUT|FAILED
		//delete file from IN
		//COMMIT
		for (File file : socj_dumb.csvFiles){
			
			System.out.println("FILE SEEN: " + file.getName() + "\n");
			
			/** loading file into memory for ETL processing." **/
			System.out.println("loading " + file.getName() + " into memory for ETL processing.");
			master_entries_rawdata.clear();
			CSVReader reader_rawdata2 = new CSVReader(new FileReader(file));
			socj_dumb.currentEntries = (ArrayList<String[]>) reader_rawdata2.readAll();
			socj_dumb.currentEntries.remove(0);  /** remove the header row, already got it above */
			master_entries_rawdata.addAll(socj_dumb.currentEntries);
		
			ArrayList<String[]> entries_rawdata = master_entries_rawdata;
			reader_rawdata2.close();
			
			System.out.println("alter CLEANSE");
			System.out.println("build STAGING");
			System.out.println("alter STAGING");
			
		    /*********************************** 
		     * SAVEPOINT #1 
		     ***********************************
		     * before CLEANSE & STAGING built 
		     ***********************************/
			System.out.println("build CLEANSE");

			Savepoint save1 = null;
			try {
				save1 = c.setSavepoint("save1");
			} catch (SQLException e1) {
				System.out.println("making savepoint 'save1' failed.");
				e1.printStackTrace();
			}
		    
		    int lastComma;
		    Savepoint createCleanseSavepoint = null;
			try {
				createCleanseSavepoint = socj_dumb.buildCleanseTable(c, socj_dumb, save1);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				System.out.println("the rollbacks failed.");
				e.printStackTrace();
			} 
		    
			String sqlaltertableAddColumn;
			PreparedStatement PS_addcolumn_Flag;
			
			try {
				socj_dumb.alterCleanseTableAddFlagColumn(c, save1, createCleanseSavepoint);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			Savepoint createStagingSavepoint = null;
		    int lastTick; //TODO shouldn't this create a new savepoint: createStagingSavepoint???
			try {
				createStagingSavepoint = socj_dumb.buildStagingTable(c, socj_dumb, save1);
			} catch (SQLException e) {
				System.out.println("rollback failed.");
				e.printStackTrace();
			}
			

		   try {
			socj_dumb.alterStagingTableAddFlagColumn(c, save1, createStagingSavepoint);
			} catch (SQLException e) {
				System.out.println("rollback failed.");
				e.printStackTrace();
			}
		    
		    
		   System.out.println("build MAPPINGS");
			/** This is defined in the file: "src-to-dst-column-mappings.properties" 
			 * and it must be configured per installation */
		   socj_dumb.generateSourceDestMappings(socj_dumb);			
		   System.out.println("MAPPING KEYS: " + StringUtils.join(socj_dumb.MAPPING_MAP.keySet(), ","));
		   System.out.println("MAPPING VALUES: "  + StringUtils.join(socj_dumb.MAPPING_MAP.values(), ","));
		   
		   /*			*/			
			
		    /***************************************** 
		     * SAVEPOINT #2 
		     *****************************************
		     * before INSERT csv records into CLEANSE
		     *****************************************/
		   Savepoint save2 = null;  
		   try {
				save2 = c.setSavepoint("save2");
			} catch (SQLException e1) {
				System.out.println("making savepoint 'save2' failed.");
				e1.printStackTrace();
			}
			
			System.out.println("insert into CLEANSE");

			PreparedStatement ps_INSERT_CLEANSE = null; 
			
			try {
				ps_INSERT_CLEANSE = c.prepareStatement(socj_dumb.buildInsertIntoCleansingTableSql(c,socj_dumb));
			} catch (SQLException e) {
				System.out.println("error creating prepared statement");
				e.printStackTrace();
			}
			
	    	try {
				socj_dumb.setAndExecuteInsertIntoCleansingTablePreparedStatement(c, socj_dumb,
						entries_rawdata, save2, ps_INSERT_CLEANSE);
			} catch (SQLException e) {
				System.out.println("error setting params and executing the insert into cleanse prep stmt.");
				e.printStackTrace();
			}
			
	    	
	    	System.out.println("RUNNING CUSTOM SQL AGAINST CLEANSING");
	    	/** TODO
		     * 
		     * !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		     * INJECT THE CUSTOM SQL AGAINST THE CLEANSE TABLE HERE
		     * !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		     * 
		     * */
		    try {
		    	Properties customCleanseSqlprops = socj_dumb.props_customsql_cleanse;
		    	int numSql = customCleanseSqlprops.size();
		    	for (int i = 1; i <= numSql; i++){
		    		String sql = customCleanseSqlprops.getProperty(String.valueOf(i));
		    		sql = sql.replace("$table", socj_dumb.src_table_name);
		    		System.out.println("CUSTOM SQL: " + sql);
		    		PreparedStatement ps = c.prepareStatement(sql);
		    		ps.execute();
		    	}
		    } catch (Exception e){
				socj_dumb.errorCleanup(save2, c, file, socj_dumb.faileddir_csvfiles, e);
				System.out.println("ETL_LOGGER: failure with custom sql against staging.");
			}
		    
			System.out.println("(copying from CLEANSE to STAGING) select insert into STAGING");

			try {
				socj_dumb.copyFromCleanseToStaging(c, socj_dumb, save2);
			} catch (SQLException e) {
				System.out.println("error occured copying from CLEANSE to STAGING.");
				e.printStackTrace();
			}
			
		   	System.out.println("FINISHED COMPLETE DONE");
	    	/** TODO
	    	 * 
	    	 * !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	    	 * INJECT THE CUSTOM SQL AGAINST THE STAGING TABLE HERE
	    	 * !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	    	 * 
	    	 * */

	    	try {
		    	Properties customStagingSqlprops = socj_dumb.props_customsql_staging;
				int numSql2 = customStagingSqlprops.size();
		    	for (int i = 1; i <= numSql2; i++){
		    		String sql = customStagingSqlprops.getProperty(String.valueOf(i));
		    		sql = sql.replace("$table", socj_dumb.dst_table_name);
		    		System.out.println("CUSTOM SQL: " + sql);
		    		PreparedStatement ps = c.prepareStatement(sql);
		    		ps.execute();
		    	}
	    	} catch (Exception e){
				socj_dumb.errorCleanup(save2, c, file, socj_dumb.faileddir_csvfiles, e);
				System.out.println("ETL_LOGGER: failure with custom sql against staging.");
			}
	   

			System.out.println("select insert into FINAL PRODUCTION");
			
	    	System.out.println("FINAL LOAD INTO THE PRODUCTION TABLE STARTING.");
	    	/** TODO
	    	 * 
	    	 * !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	    	 * INJECT THE CUSTOM TRANSFER SQL AGAINST THE FINAL PRODUCTION TABLE HERE
	    	 * !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	    	 * 
	    	 * */
	    	//INSERT INTO $staging_table ($columnlist) SELECT $prodcolumnlist FROM $staging_table
			socj_dumb.prod_table_name = socj_dumb.props_etlconfig.getProperty("tableNamePattern");
	    	String insertSelect_Production = "INSERT INTO " + socj_dumb.prod_table_name + " (_columnlist_) SELECT _columnlist_ FROM " + socj_dumb.dst_table_name;
	    	
	    	//TODO: don't want to build this each time. enhance
	    	String columnlist = StringUtils.join(socj_dumb.DEST_COLTYPE_MAP.keySet(), ","); 
	    	System.out.println("ETL_LOGGER(columnList): " + columnlist);

	    	insertSelect_Production = insertSelect_Production.replaceAll("_columnlist_", columnlist);
	    	System.out.println("ETL_LOGGER(insertSelect_Production): " + insertSelect_Production);
	    	
	    	try {
	    		PreparedStatement ps_finalToProd = c.prepareStatement(insertSelect_Production);
	    		ps_finalToProd.execute();
	    	} catch (Exception e) {
	    		System.out.println("ETL_LOGGER: OOOOOOPS SOMETHING HAPPENED BAD IN THE END. SHUCKS");
	    		socj_dumb.errorCleanup(save2, c, file, socj_dumb.faileddir_csvfiles, e);
	    	}
	    	
	    	try {
		    	Properties customProdLoaderSqlprops = socj_dumb.props_customsql_final_to_prod;
				int numSql3 = customProdLoaderSqlprops.size();
		    	for (int i = 1; i <= numSql3; i++){
		    		String sql = customProdLoaderSqlprops.getProperty(String.valueOf(i));
		    		sql = sql.replace("$table", socj_dumb.dst_table_name);
		    		System.out.println("CUSTOM SQL: " + sql);
		    		PreparedStatement ps = c.prepareStatement(sql);
		    		ps.execute();
		    	}
	    	} catch (Exception e){
	    		socj_dumb.errorCleanup(save2, c, file, socj_dumb.faileddir_csvfiles, e);
				System.out.println("ETL_LOGGER: failure with custom sql final to prod: " + e.getMessage());
			}

			System.out.println("--CLEANUP--");
			System.out.println("copy file to OUT|FAILED");
			System.out.println("delete file from IN");
			
    		try{
    			/** Destination dir */
    			File destinationDir = new File(socj_dumb.outputdir_csvfiles);
    			// ON FAILURE: destinationDir = faileddir_csvfiles
    			
		    	etlMoveFile(file, destinationDir);

		    	System.out.println("COMMIT");
		    	c.commit();
    		} catch (IOException io){
    			System.out.println("ALERT PROBLEM DELETING FILE: " + io.getMessage());
    		} catch (SQLException sqle){
    			System.out.println("ALERT PROBLEM COMMITTING: " + sqle.getMessage());
    		}
			
		}
		try {
			if (c != null){
				c.close();
			}
		} catch (SQLException e){
			System.out.println("uh oh. bombed trying to close sql connection");
		}
	}

}
