package ch.unibas.dmi.dbis.dis.mom.exception;

/**
 * Exception that is thrown if an account is already known.
 * 
 * @author Lukas Beck, HS 2012
 */
public class KnownAccountException extends Exception {
	private static final long serialVersionUID = 3933243801682108980L;
	private String iban;
	
	public KnownAccountException(String iban){
		super("Account "+ iban);
		this.iban = iban;
	}
	
	public String getIban() {
		return this.iban;
	}
}
