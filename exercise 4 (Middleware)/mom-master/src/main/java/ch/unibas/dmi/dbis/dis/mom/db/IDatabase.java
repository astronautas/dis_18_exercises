package ch.unibas.dmi.dbis.dis.mom.db;

import java.util.Set;

import ch.unibas.dmi.dbis.dis.mom.exception.AccountOverdrawException;
import ch.unibas.dmi.dbis.dis.mom.exception.KnownAccountException;
import ch.unibas.dmi.dbis.dis.mom.exception.UnknownAccountException;

//TODO: add IllegalArgumentException when amount is non-positive
//TODO: add IllegalArgumentException when iban is empty
//TODO: add NullPointerException when iban is null

/**
 * Interface for the persistence layer of typical bank operations.
 * Classes that implement this interface should make all changes persistent.
 * 
 * @author Filip-M. Brinkmann University of Basel, AS 2011
 * @author Lukas Beck, HS 2012
 */
public interface IDatabase {
	/**
	 * @param iban account identifier
	 * @return balance of the specified account
	 * @throws UnknownAccountException If the account does not exist
	 */
	public double getBalance( String iban ) throws UnknownAccountException;
	
	/**
	 * Deposits the specified amount on the specified account.
	 * 
	 * @param iban account identifier
	 * @param amount amount to deposit
	 * @throws UnknownAccountException If the account does not exist
	 */
	public void deposit( String iban, double amount ) throws UnknownAccountException;
	
	/**
	 * Withdraws the specified amount from the specified account.
	 * 
	 * @param iban account identifier
	 * @param amount amount to withdraw
	 * @throws UnknownAccountException If the account does not exist
	 * @throws AccountOverdrawException If the account has less than {@code amount} balance
	 */
	public void withdraw( String iban, double amount ) throws UnknownAccountException, AccountOverdrawException;
	
	/**
	 * Adds an account to the database with a given balance.
	 * 
	 * @param iban account identifier
	 * @param balance initial balance of the account
	 * @throws KnownAccountException If an account with the given {@code iban} already exists
	 */
	public void addAccount( String iban, double balance ) throws KnownAccountException;
	
	/**
	 * Removes an account from the database.
	 * 
	 * @param iban account identifier
	 * @throws UnknownAccountException If the account does not exist
	 */
	public void deleteAccount( String iban ) throws UnknownAccountException;
	
	/**
	 * @return the set of all existing account identifiers
	 */
	public Set<String> listAccounts();
}
