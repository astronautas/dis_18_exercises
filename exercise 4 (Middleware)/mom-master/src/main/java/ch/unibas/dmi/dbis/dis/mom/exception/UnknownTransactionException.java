package ch.unibas.dmi.dbis.dis.mom.exception;

public class UnknownTransactionException extends Exception {
	private static final long serialVersionUID = 7692510943578593684L;
	private String id;
	
	public UnknownTransactionException(String id) {
		super("Transaction " + id);
		this.id = id;
	}
	
	public String getId() {
		return this.id;
	}
}
