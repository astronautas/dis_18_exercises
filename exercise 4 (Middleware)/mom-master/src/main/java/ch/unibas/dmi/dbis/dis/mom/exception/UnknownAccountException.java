package ch.unibas.dmi.dbis.dis.mom.exception;

/**
 * Exception to throw when an IBAN is unknown to the bank.
 * It holds the bank identifier as well as the account identifier that was unknown.
 * 
 * @author Filip-M. Brinkmann, AS 2011
 * @author Lukas Beck, HS 2012
 */
public class UnknownAccountException extends Exception {
	private static final long serialVersionUID = 5425973196539486126L;
	public static final String FORMAT = "Account %s of bank %s";
	private static final char SEPARATOR = ':';
	private String iban;
	private String bic;
	
	public UnknownAccountException(String bic, String iban){
		super(String.format(FORMAT, iban, bic));
		this.iban = iban;
		this.bic = bic;
	}
	
	public String createString() {
		return this.bic + SEPARATOR + this.iban;
	}
	
	public static UnknownAccountException parseString(String s) {
		String[] split = s.split(String.valueOf(SEPARATOR));
		if (split.length != 2) {
			throw new IllegalArgumentException("String has to have a separator " + SEPARATOR);
		}
		
		return new UnknownAccountException(split[0], split[1]);
	}

	public String getIban() {
		return this.iban;
	}
	
	public String getBic() {
		return this.bic;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((bic == null) ? 0 : bic.hashCode());
		result = prime * result + ((iban == null) ? 0 : iban.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof UnknownAccountException)) {
			return false;
		}
		UnknownAccountException other = (UnknownAccountException) obj;
		if (bic == null) {
			if (other.bic != null) {
				return false;
			}
		} else if (!bic.equals(other.bic)) {
			return false;
		}
		if (iban == null) {
			if (other.iban != null) {
				return false;
			}
		} else if (!iban.equals(other.iban)) {
			return false;
		}
		return true;
	}
}
