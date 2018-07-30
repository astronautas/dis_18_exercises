package ch.unibas.dmi.dbis.dis.mom.db;

import ch.unibas.dmi.dbis.dis.mom.exception.TransactionExistsException;
import ch.unibas.dmi.dbis.dis.mom.server.Transaction;

/**
 * Abstract class that implements {@code put} of {@link ITransactionTable}.
 * Additionally, it provides functionality to check if a given transaction expired.
 * 
 * @author Lukas Beck, HS 2012
 */
public abstract class TransactionTable implements ITransactionTable {
	private static final long TRANSACTION_TIMEOUT = 60000;
	private String bic;

	/**
	 * Constructs a new {@link TransactionTable} with a given bank identifier.
	 * This identifier is used to generate unique transaction ids.
	 * 
	 * @param bic bank identifier
	 */
	public TransactionTable(String bic) {
		this.bic = bic;
	}

	/**
	 * Makes the value of the id counter persistent. 
	 * @param idCounter value of the id counter
	 */
	protected abstract void putIdCounter(int idCounter);
	
	/**
	 * @return value of the persistent id counter
	 */
	protected abstract int getIdCounter();
	
	/**
	 * @return bank identifier assigned to this transaction table
	 */
	protected String getBic() {
		return this.bic;
	}
	
	@Override
	public String put(Transaction tx) {	
		String id = generateTransactionId();
		try {
			this.put(id, tx);
		} catch (TransactionExistsException e) {
			throw new IllegalStateException("The generated transaction id '" + id + "' was not unique.");
		}
		return id;
	}

	private String generateTransactionId() {
		String id = bic + "_" + incrementAndReturnCounter();
		
		while (this.containsId(id)) {
			id = bic + "_" + incrementAndReturnCounter();
		}
		
		return id;
	}

	private synchronized int incrementAndReturnCounter() {
		int _return = this.getIdCounter();
		this.putIdCounter(++_return);
		return _return;
	}

	/**
	 * Checks, if a given {@link Transaction} expired
	 * by using a default timeout value {@code TRANSACTION_TIMEOUT}.
	 * 
	 * @param tx transaction
	 * @return true, if the given transaction expired
	 */
	public static boolean isTransactionExpired(Transaction tx) {
		return System.currentTimeMillis() - tx.startTime > TRANSACTION_TIMEOUT;
	}
}
