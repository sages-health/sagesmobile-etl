/**
 * 
 */
package org.jhuapl.edu.sages.etl;

/**
 * @author POKUAM1
 * @created Oct 14, 2011
 */
public class SagesEtlException extends Exception {

	/***/
	private static final long serialVersionUID = 1435231489770623684L;

	/**
	 * 
	 */
	public SagesEtlException() {
		super();
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param message
	 * @param cause
	 */
	public SagesEtlException(String message, Throwable cause) {
		super(message, cause);
	}

	/**
	 * @param message
	 */
	public SagesEtlException(String message) {
		super(message);
	}

	/**
	 * @param cause
	 */
	public SagesEtlException(Throwable cause) {
		super(cause);
	}

	
}
