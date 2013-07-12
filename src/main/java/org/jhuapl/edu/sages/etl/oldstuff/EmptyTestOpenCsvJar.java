package org.jhuapl.edu.sages.etl.oldstuff;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.jhuapl.edu.sages.etl.ConnectionFactory;
import org.jhuapl.edu.sages.etl.ETLProperties;
import org.jhuapl.edu.sages.etl.SagesEtlException;

import au.com.bytecode.opencsv.CSVReader;

/**
 * @author POKUAM1
 * @created Oct 4, 2011
 */
public class EmptyTestOpenCsvJar {

	private File[] csvFiles;
	private File currentFile;
	private File fileMarkedForDeletion;
	private ArrayList<String[]> currentEntries;
	private int currentRecNum;
	//private static List<File> failedCsvFiles;
	private boolean success;
	
	
	/** csv files are loaded from inputdir and moved to outputdir after being processed successfully  */
	private String inputdir_csvfiles;
	private String outputdir_csvfiles;
	private String faileddir_csvfiles;
	


		
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
	public EmptyTestOpenCsvJar(String dbms, int portNumber, String userName,
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
	 * @throws SagesEtlException 
	 */
	public EmptyTestOpenCsvJar() throws SagesEtlException {
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
		EmptyTestOpenCsvJar tocj = null;
		
		try {
		tocj = new EmptyTestOpenCsvJar();
		} catch (SagesEtlException e){
			throw abort("Unable to load properties",e );
		}
		c = tocj.getConnection();
		System.out.println("catalog: " + c.getCatalog());
		
		// LOAD PROPERTIES
		// ESTABLISH DB CONNECTION
		// TOGGLE: TRANSACTIONS, LOGGING, ETC.
		// LOAD CSV FILES
		// ETL STEPS per CSV (move, delete on success)
		// UNLOAD FILES
		
		CSVReader reader_rawdata = new CSVReader(new FileReader("C:\\dev\\git-repositories\\sandbox\\sandbox\\unittests\\datachunks\\modded_formdata_oevisit11106-1715.csv"));
		
		/** csv files are loaded from inputdir and moved to outputdir after being processed successfully  */
//		String inputdir_csvfiles = tocj.props_etlconfig.getProperty("csvinputdir");
//		String outputdir_csvfiles = tocj.props_etlconfig.getProperty("csvoutputdir");
//		String faileddir_csvfiles = tocj.props_etlconfig.getProperty("csvfaileddir");
		
		ArrayList<String[]> master_entries_rawdata = new ArrayList<String[]>();

		extractHeaderColumns(tocj);
		
		
		/** load into memory all csv files in the inputdir
		 *  - each iteration is new transaction so we can isolate erroneous files 
		 **/
		String pathin = tocj.inputdir_csvfiles;
		File dirin = new File(pathin);
		File[] filesin = dirin.listFiles();
		for (File file : filesin){
			tocj.success = false;
			master_entries_rawdata.clear();
//notused			tocj.currentFile = file; //TODO currentEntries, currentRecNum
			
/*			if (tocj.fileMarkedForDeletion != null) {
				FileUtils.forceDelete(tocj.fileMarkedForDeletion);
				c.commit();
				tocj.fileMarkedForDeletion = null;
			}*/
			
			CSVReader reader_rawdata2 = new CSVReader(new FileReader(file));
			tocj.currentEntries = (ArrayList<String[]>) reader_rawdata2.readAll();
			reader_rawdata2.close();
			tocj.currentEntries.remove(0);  /** remove the header row */
			master_entries_rawdata.addAll(tocj.currentEntries);
		
			ArrayList<String[]> entries_rawdata = master_entries_rawdata;
			FileUtils.forceDelete(file);
    	
/*
    		try{
		    	Date date = new Date();
		    	long dtime = date.getTime();
		    	*//** Destination dir *//*
		    	File dir = new File(outputdir_csvfiles);
		    	*//** Move file to new dir *//*
		    	FileUtils.copyFile(file, new File(dir, dtime + "_"+ file.getName()));
		    	//boolean success = file.renameTo(new File(dir, dtime + "_"+ file.getName()));
		    	Thread.sleep(2000);
		    	//deletethismess = new File(file.getPath());
		    	//FileUtils.forceDelete(deletethismess);
		    	FileUtils.forceDelete(file);
		    	//c.commit();
    		} catch (IOException io){
    			System.out.println("CRAP THAT'S NOT GOOD COULDNT MOVE/DELETE FILE TO OUTPUT: " + io.getMessage());
    		}*/
		}
	}

	/**
	 * Establishes database connection to the target database
	 * @return Connection
	 * @throws SQLException
	 */
	public Connection getConnection() throws SagesEtlException {
		try {
			return ConnectionFactory.createConnection(this.dbms, this.serverName, this.dbName,
					this.userName, this.password, this.portNumber);
		} catch (SQLException e){
			throw abort("Sorry, failed to establish database connection", e);
		}
    }
	
	/**
	 * @param tocj
	 * @return
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	protected static void extractHeaderColumns(EmptyTestOpenCsvJar tocj)
			throws FileNotFoundException, IOException {
		/** header columns used to define the CLEANSE table schema */
		header_src = new String[0];
		//failedCsvFiles = new ArrayList<File>();
		//File[] readCsvFiles = new File[0];
		
		/** determine header columns **/
		File csvinputDir = new File(tocj.inputdir_csvfiles);
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
		reader_rawdata2.close();
		return headerColumns;
	}


}
