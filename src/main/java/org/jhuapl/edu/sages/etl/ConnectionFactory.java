/**
 * 
 */
package org.jhuapl.edu.sages.etl;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * @author POKUAM1
 * @created Oct 28, 2011
 */
public class ConnectionFactory {
	
	public ConnectionFactory(){
	}
	
	/**
	 * Establishes database connection to the target database
	 * @return Connection
	 * @throws SQLException
	 */
	public static Connection createConnection(String dbmsType, String serverName, String dbName, String user, String password, int portNumber) throws SQLException{
	    Connection con = null; 
	    
		Properties connectionProps = new Properties();
	    connectionProps.put("user", user);
	    connectionProps.put("password", password);
	    
	    if (ETLProperties.dbid_mysql.equals(dbmsType)) {
	    	con = DriverManager.getConnection("jdbc:" + dbmsType + "://" + serverName + ":" + portNumber + "/", connectionProps);
	    } else if (ETLProperties.dbid_msaccess.equals(dbmsType)) {
	    	//http://www.javaworld.com/javaworld/javaqa/2000-09/03-qa-0922-access.html
	    	// jdbc:odbc:<NAME>
	    	try {
				Class.forName("sun.jdbc.odbc.JdbcOdbcDriver");
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    	//http://www.planet-source-code.com/vb/scripts/ShowCode.asp?txtCodeId=2691&lngWId=2
	    	
	    	//String url = "C:\\Documents and Settings\\POKUAM1\\My Documents\\mdbtestdjib.mdb";
	    	String url = "C:\\Documents and Settings\\POKUAM1\\My Documents\\testdjib.accdb";
	    	File file = new File(url);
	    	String fileTypes = "*.mdb";
	    	//String jdbcUrl = "jdbc:odbc:Driver={Microsoft Access Driver (" + fileTypes + ")};DBQ=" + file.getAbsolutePath();
	    	String jdbcUrl = "jdbc:odbc:Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=" + file.getAbsolutePath();
	    	System.out.println("jdbcurl: " + jdbcUrl);
	    	con = DriverManager.getConnection(jdbcUrl);
	    	//con = DriverManager.getConnection("jdbc:odbc:" + this.dbms);
	    	System.out.println("Connection ok.");
	    } else if (ETLProperties.dbid_postgresql.equals(dbmsType)) {
	    	con = DriverManager.getConnection("jdbc:" + dbmsType + "://" + serverName + ":" + portNumber + "/" + dbName, connectionProps);
	    }
	    //TODO: DO AS LOGGING
	    System.out.println("Connected to database");
	    con.setAutoCommit(false);
	    return con;
	}
	

}
