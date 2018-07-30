package ch.unibas.dmi.dbis.dis.mom.test;

import java.util.Iterator;
import java.util.Random;

import org.junit.Assert;
import org.junit.rules.ExpectedException;

import ch.unibas.dmi.dbis.dis.mom.db.IDatabase;
import ch.unibas.dmi.dbis.dis.mom.exception.KnownAccountException;
import ch.unibas.dmi.dbis.dis.mom.exception.UnknownAccountException;
import ch.unibas.dmi.dbis.dis.mom.server.BankServer;

/**
 * Utility functions for testing.
 * 
 * @author Lukas Beck, HS 2012
 */
public class TestUtilities {
	private static final double DELTA = 0.0001;
	private static final long DEFAULT_MILLIS = 5000;
	private static final int NUMBER_OF_ACCOUNTS = 10;
	private static final int MAX_ACCOUNT_BALANCE = 50000;
	private static Random random = new Random();
	
	/**
	 * A convenience function to test double equality with a default delta value.
	 * It calls internally the JUnit function {@assertEquals} from {@link Assert}.
	 * 
	 * @param message the identifying message for the AssertionError ({@code null} okay)
	 * @param expected expected value
	 * @param actual the value to check against expected
	 */
	public static void assertEquals(String message, double expected, double actual) {
		Assert.assertEquals(message, expected, actual, DELTA);
	}
	
	/**
	 * A convenience function to set the {@link ExpectedException} to {@code type}.
	 * 
	 * @param thrown {@link ExpectedException} object
	 * @param type {@link Class} of the expected exception
	 */
	public static void expectException(ExpectedException thrown, Class<? extends Exception> type) {
		thrown.expect(type);
	}
	
	/**
	 * A convenience function to set the {@link ExpectedException} to {@code type}
	 * with the expected {@code message}. When using this function,
	 * the {@code message} is not tested on equality but only if the exception contains the {@code message}.
	 * 
	 * @param thrown {@link ExpectedException} object
	 * @param type {@link Class} of the expected exception
	 * @param message {@link String} that should contain the expected exception.
	 */
	public static void expectException(ExpectedException thrown, Class<? extends Exception> type, String message) {
		expectException(thrown, type);
		thrown.expectMessage(message);
	}

	/**
	 * A convenience function to set the {@link ExpectedException} to {@link UnknownAccountException}
	 * with the given {@code iban} and {@code bic}.
	 * 
	 * @param thrown {@link ExpectedException} object
	 * @param iban the unknown account identifier
	 * @param bic identifier of the bank where {@code iban} is unknown
	 */
	public static void expectUnknownAccountException(ExpectedException thrown, String iban, String bic) {
		expectException(thrown, UnknownAccountException.class,
				String.format(UnknownAccountException.FORMAT, iban, bic));
	}	
	
	/**
	 * A convenience function to sleep with a default value.
	 * This method returns immediately if this thread is interrupted
	 * and does not throw any exception
	 */
	public static void sleepQuiet() {
		sleepQuiet(DEFAULT_MILLIS);
	}
	
	/**
	 * A convenience function to sleep {@code millis} milliseconds.
	 * This method returns immediately if this thread is interrupted
	 * and does not throw any exception
	 * 
	 * @param millis the length of time to sleep in milliseconds
	 * @throws IllegalArgumentException If the value of {@code millis} is negative
	 */
	public static void sleepQuiet(long millis) {
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {}
	}

	/**
	 * This method inserts randomly generated test data into the provided bank server.
	 * However, only new accounts are inserted if there less than 2 accounts listed in the database.
	 * 
	 * @param server bank server
	 * @return {@link String} array that contains two existing account identifiers
	 */
	public static String[] insertTestData(BankServer server) {
		return insertTestData(server, false);
	}
	
	/**
	 * This method inserts generated test data into the provided bank server.
	 * However, only new accounts are inserted if there less than 2 accounts listed in the database.
	 * 
	 * 
	 * @param server bank server
	 * @param useHardcodedData if true, data with always the same account identifiers is inserted
	 * @return {@link String} array that contains two existing account identifiers
	 */
	public static String[] insertTestData(BankServer server, boolean useHardcodedData) {
		System.out.print("Inserting test data...");
		IDatabase db = server.getDatabase();
		
		if (db.listAccounts().size() < 2 || useHardcodedData) {
			String accountIbanPrefix;
			if (useHardcodedData) {
				accountIbanPrefix = "iban";
			}
			else {
				accountIbanPrefix = String.valueOf(System.nanoTime());
			}
			
			for (int i = 0; i < NUMBER_OF_ACCOUNTS; i++) {
				String iban = accountIbanPrefix + String.valueOf(i);	
				double balance = 1000 + random.nextInt(MAX_ACCOUNT_BALANCE);			
				
				try {
					db.addAccount(iban, balance);
				} catch (KnownAccountException e) {
					// ignore
				}
			}
		}
		
		String[] accounts = new String[2];
		Iterator<String> iter = db.listAccounts().iterator();
		accounts[0] = iter.next();
		accounts[1] = iter.next();
		
		System.out.println(" Done!");
		
		return accounts;
	}
	
	/**
	 * This method inserts test data into the provided bank servers.
	 * However, only new accounts are inserted if there less than 2 accounts listed in the databases.
	 * 
	 * @param server1 first bank server
	 * @param server2 second bank server
	 * @return {@link String} array that contains 4 account identifiers. 
	 * 	The first two are related to {@code server1}, the second two to {@code server2}.
	 */
	public static String[] insertTestData(BankServer server1, BankServer server2) {
		System.out.print("Inserting test data...");
		IDatabase db1 = server1.getDatabase();
		IDatabase db2 = server2.getDatabase();
		
		
		
		if (db1.listAccounts().size() < 2 || db2.listAccounts().size() < 2) {	
			final long accountIbanPrefix = System.nanoTime();
			for (int i = 0; i < NUMBER_OF_ACCOUNTS; i++) {
				String iban = String.valueOf(accountIbanPrefix) + String.valueOf(i);	
				double balance = 1000 + random.nextInt(MAX_ACCOUNT_BALANCE);			
				
				IDatabase db;
				if (i % 2 == 0) {
					db = db1;
				}
				else {
					db = db2;
				}
				try {
					db.addAccount(iban, balance);
				} catch (KnownAccountException e) {
					// ignore
				}
			}
		}				
		
		String accounts[] = new String[4];
		Iterator<String> iter1 = db1.listAccounts().iterator();
		accounts[0] = iter1.next();
		accounts[1] = iter1.next();
		
		Iterator<String> iter2 = db2.listAccounts().iterator();
		accounts[2] = iter2.next();
		accounts[3] = iter2.next();		
		
		System.out.println(" Done!");
		
		return accounts;
	}
}
