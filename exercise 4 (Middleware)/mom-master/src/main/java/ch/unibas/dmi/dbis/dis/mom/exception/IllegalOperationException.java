package ch.unibas.dmi.dbis.dis.mom.exception;

/**
 * Exception to throw when an operation is malformed or unknown.
 * 
 * @author Filip-M. Brinkmann
 *  University of Basel, AS 2011
 */
public class IllegalOperationException extends Exception {
	private static final long serialVersionUID = -2675905551849758005L;

	public IllegalOperationException(String reason) {
		super(reason);
	}
}
