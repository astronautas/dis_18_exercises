package ch.unibas.dmi.dbis.dis.mom.db;

import java.util.Map.Entry;
import java.util.Set;

import ch.unibas.dmi.dbis.dis.mom.exception.TransactionExistsException;
import ch.unibas.dmi.dbis.dis.mom.exception.UnknownTransactionException;
import ch.unibas.dmi.dbis.dis.mom.server.Transaction;

//TODO: add IllegalArgumentException when id is empty
//TODO: add NullPointerException when id or tx is null

/**
 * Interface to the transaction table.
 * 
 * @author Lukas Beck, HS 2012
 */
public interface ITransactionTable {
	/**
	 * Generates and returns a transaction id
	 * while inserting the {@link Transaction} with its generated id.
	 * 
	 * @param tx {@link Transaction} to insert
	 * @return generated id
	 */
	public String put( Transaction tx );
	
	/**
	 * Inserts a {@link Transaction} with a explicit id.
	 * 
	 * @param id id of transaction
	 * @param tx transaction
	 * @throws TransactionExistsException If the id already exists
	 */
	public void put( String id, Transaction tx ) throws TransactionExistsException;
	
	/**
	 * Returns a specific {@link Transaction}.
	 * 
	 * @param id id of transaction
	 * @return transaction of id
	 * @throws UnknownTransactionException If transaction with id does not exist
	 */
	public Transaction get( String id ) throws UnknownTransactionException;
	
	/**
	 * Removes a specific {@link Transaction}.
	 * 
	 * @param id id of transaction
	 * @throws UnknownTransactionException If transaction with id does not exist
	 */
	public void remove( String id ) throws UnknownTransactionException;
	
	/**
	 * @param id the id
	 * @return true, if the transaction id is in the table 
	 */
	public boolean containsId( String id );
	
	/**
	 * @return Set of entries in the table. The set contains all ids and their transaction. 
	 */
	public Set<Entry<String, Transaction>> list();
}
