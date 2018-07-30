package ch.unibas.dmi.dbis.dis.mom.exception;

import ch.unibas.dmi.dbis.dis.mom.server.Transaction;

/**
 * Exception that is thrown if a {@link Transaction} expired.
 * It holds the id and the {@link Transaction} object.
 * 
 * @author Lukas Beck, HS 2012
 */
public class TransactionExpiredException extends Exception {
	private static final long serialVersionUID = 3866721792820087097L;
	private String id;
	private Transaction tx;
	
	public TransactionExpiredException(String id, Transaction tx) {
		super("Transaction {" + id + " : " + tx + "} expired"); 
		this.id = id;
		this.tx = tx;
	}
	
	public String getId() {
		return id;
	}

	public Transaction getTransaction() {
		return tx;
	}
}
