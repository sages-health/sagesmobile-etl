/**
 * 
 */
package org.jhuapl.edu.sages.etl;

/**
 * Loads the properties files that configure the system
 * 
 * @author POKUAM1
 * @created Oct 28, 2011
 */
public interface PropertiesLoader {

	public void loadEtlProperties() throws SagesEtlException;
}
