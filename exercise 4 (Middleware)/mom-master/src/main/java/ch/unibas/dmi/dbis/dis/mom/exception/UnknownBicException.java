package ch.unibas.dmi.dbis.dis.mom.exception;


/**
 * Exception to throw when a BIC is unknown to the bank.
 * 
 * @author Filip-M. Brinkmann
 *  University of Basel, AS 2011
 */
public class UnknownBicException extends Exception {
	private static final long serialVersionUID = 7534198639296910890L;

	public UnknownBicException(String bic){
		super("bic "+bic);
	}
}
