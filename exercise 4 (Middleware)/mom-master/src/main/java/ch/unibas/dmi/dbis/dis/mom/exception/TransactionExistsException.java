package ch.unibas.dmi.dbis.dis.mom.exception;

import ch.unibas.dmi.dbis.dis.mom.server.Transaction;

public class TransactionExistsException extends Exception {
	private static final long serialVersionUID = 5546896747069410625L;
	private String id;
	private Transaction tx;
	
	public TransactionExistsException(String id, Transaction tx) {
		super("Transaction " + id + ": " + tx);
		this.id = id;
		this.tx = tx;
	}
	
	public String getId() {
		return this.id;
	}
	
	public Transaction getTransaction() {
		return this.tx;
	}
}
