package ch.unibas.dmi.dbis.dis.mom.exception;

/**
 * Exception to throw when an account has not enough money in it.
 * 
 * @author Filip-M. Brinkmann
 *  University of Basel, AS 2011
 * @author Lukas Beck, HS 2012
 */
public class AccountOverdrawException extends Exception {
	private static final long serialVersionUID = 7567721941370431647L;
	private String iban;
	
	public AccountOverdrawException(String iban){
		super("Account "+ iban);
		this.iban = iban;
	}
	
	public String getIban() {
		return this.iban;
	}
}
