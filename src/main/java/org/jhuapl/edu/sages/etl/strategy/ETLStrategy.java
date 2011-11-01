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

/**
 * @author POKUAM1
 * @created Nov 1, 2011
 */
public interface ETLStrategy {

	void extractHeaderColumns(SagesOpenCsvJar socj) throws FileNotFoundException, IOException;
	
	Savepoint buildCleanseTable(Connection c, SagesOpenCsvJar socj, Savepoint save1) throws SQLException,SagesEtlException;
	void alterCleanseTableAddFlagColumn(Connection c, Savepoint save1, Savepoint createCleanseSavepoint) throws SQLException, SagesEtlException;

	Savepoint buildStagingTable(Connection c, SagesOpenCsvJar socj, Savepoint save1) throws SQLException, SagesEtlException;
	void alterStagingTableAddFlagColumn(Connection c, Savepoint save1, Savepoint createCleanseSavepoint) throws SQLException, SagesEtlException;
	
	void generateSourceDestMappings(SagesOpenCsvJar socj);
	
	String buildInsertIntoCleansingTableSql(Connection c, SagesOpenCsvJar socj) throws SQLException;
	void setAndExecuteInsertIntoCleansingTablePreparedStatement(Connection c, SagesOpenCsvJar socj, 
			ArrayList<String[]> entries_rawdata, Savepoint save2,
			PreparedStatement ps_INSERT_CLEANSE) throws SQLException;

	void copyFromCleanseToStaging(Connection c, SagesOpenCsvJar socj, Savepoint save2) throws SQLException;

	int errorCleanup(Savepoint savepoint, Connection connection, File currentCsv, String failedDirPath, Exception e);

	/*****ORIGINALS BELOW WITH TOCJ ******/
	
	void extractHeaderColumns(TestOpenCsvJar tocj)throws FileNotFoundException, IOException;
	
	String[] determineHeaderColumns(File file) throws FileNotFoundException, IOException;
	
	String addFlagColumn(String tableToModify);
	
	Savepoint buildCleanseTable(Connection c, TestOpenCsvJar tocj, Savepoint save1) throws SQLException,SagesEtlException;
//	void alterCleanseTableAddFlagColumn(Connection c, Savepoint save1, Savepoint createCleanseSavepoint) throws SQLException, SagesEtlException;
	
	void buildStagingTable(Connection c, TestOpenCsvJar tocj, Savepoint save1) throws SQLException, SagesEtlException;
//	void alterStagingTableAddFlagColumn(Connection c, Savepoint save1, Savepoint createCleanseSavepoint) throws SQLException, SagesEtlException;
	void generateSourceDestMappings(TestOpenCsvJar tocj);

}
