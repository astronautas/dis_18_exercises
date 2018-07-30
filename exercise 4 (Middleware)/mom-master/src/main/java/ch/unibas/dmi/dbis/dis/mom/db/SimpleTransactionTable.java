package ch.unibas.dmi.dbis.dis.mom.db;

import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map.Entry;
import java.util.Set;

import ch.unibas.dmi.dbis.dis.mom.exception.TransactionExistsException;
import ch.unibas.dmi.dbis.dis.mom.exception.UnknownTransactionException;
import ch.unibas.dmi.dbis.dis.mom.server.BankServer;
import ch.unibas.dmi.dbis.dis.mom.server.Transaction;

/**
 * Mockup transaction table for usage with {@link BankServer}.
 * 
 * @author Lukas Beck, HS 2012
 */
public class SimpleTransactionTable extends TransactionTable {
	private Hashtable<String, Transaction> table = new Hashtable<String, Transaction>();
	private Integer idCounter = new Integer(0);
	
	public SimpleTransactionTable(String bic) {
		super(bic);
	}
	
	@Override
	public void put(String id, Transaction tx) throws TransactionExistsException {
		synchronized (table) {
			if (this.containsId(id)) {
				throw new TransactionExistsException(id, table.get(id));
			}
			
			table.put(id, tx);
		}		
	}

	@Override
	public Transaction get(String id) throws UnknownTransactionException {
		synchronized (table) {
			if (!this.containsId(id)) {
				throw new UnknownTransactionException(id);
			}
			
			return table.get(id);
		}
	}

	@Override
	public Set<Entry<String, Transaction>> list() {
		return new HashSet<Entry<String, Transaction>>(table.entrySet());
	}

	@Override
	public boolean containsId(String id) {
		return table.containsKey(id);
	}

	@Override
	public void remove(String id) throws UnknownTransactionException {
		if (!table.containsKey(id)) {
			throw new UnknownTransactionException(id);
		}
		
		table.remove(id);
	}

	@Override
	protected void putIdCounter(int idCounter) {
		synchronized (this.idCounter) {
			this.idCounter = new Integer(idCounter);
		}
	}

	@Override
	protected int getIdCounter() {
		synchronized (idCounter) {
			return this.idCounter.intValue();
		}		
	}
}
