package ch.unibas.dmi.dbis.dis.mom.test;

import static org.junit.Assert.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Set;

import org.junit.*;
import org.junit.rules.ExpectedException;

import ch.unibas.dmi.dbis.dis.mom.db.IDatabase;
import ch.unibas.dmi.dbis.dis.mom.exception.AccountOverdrawException;
import ch.unibas.dmi.dbis.dis.mom.exception.KnownAccountException;
import ch.unibas.dmi.dbis.dis.mom.exception.UnknownAccountException;
import ch.unibas.dmi.dbis.dis.mom.server.BankServer;

/**
 * JUnitTest that tests the functionality of the given implementation of {@link IDatabase}
 * provided by {@link BankServer#createDatabase(String, com.amazonaws.auth.AWSCredentials) createDatabase}
 * from {@link BankServer}.
 * This test does also check the persistence of the database implementation.
 * 
 * @author Lukas Beck, HS2012
 */
public class DatabaseTest {
	private static final int SLEEP_TIME = 125;
	private static final int TIMEOUT = 10*1000;
	private static final String BIC = "databaseTest";		
	private static final String IBAN1 = "test1";
	private static final String IBAN2 = "test2";	
	private static final double BALANCE1 = 543.21;
	private static final double BALANCE2 = 123.45;
	private static final String UNKNOWN = "unknown_account_iban";
	private static IDatabase database;
	
	@Rule
	public ExpectedException thrown = ExpectedException.none();
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		database = getDatabase();
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		
	}

	@Before
	public void setUp() throws Exception {
		blockingAddAccount(IBAN1, BALANCE1);
		blockingAddAccount(IBAN2, BALANCE2);
	}

	@After
	public void tearDown() throws Exception {
		deleteAllAccounts();
		Set<String> accounts = database.listAccounts();
		assertEquals("After trying to delete all accounts, there are still accounts available:\n"+accounts, 0, accounts.size());
	}

	private static IDatabase getDatabase() throws FileNotFoundException, IOException {
		//return new SimpleDatabase(BIC);
		return BankServer.createDatabase(BIC, BankServer.createCredentials());
	}
	
	private static void deleteAllAccounts() throws UnknownAccountException, InterruptedException {
		Set<String> list;
		while ((list = database.listAccounts()).size() > 0) {
			for (String id : list) {
				blockingDeleteAccount(id);
			}
			Thread.sleep(1000);
		}
	}
	
	private static void blockingAddAccount(String iban, double balance) throws KnownAccountException, InterruptedException {
		try {
			database.addAccount(iban, balance);
		} catch (KnownAccountException e) {
			try {
				TestUtilities.assertEquals("Account already exists but has the wrong balance", balance, database.getBalance(iban));
				return;
			} catch (UnknownAccountException e1) {}			
		}
		
		long start = System.currentTimeMillis();
		while (System.currentTimeMillis() - start < TIMEOUT) {
			try {
				Thread.sleep(SLEEP_TIME);
			} catch (InterruptedException e) {}
			try {
				double newBalance = database.getBalance(iban);
				TestUtilities.assertEquals("newly created account has wrong balance", balance, newBalance);
				return;
			} catch (UnknownAccountException e) {}
		}
		
		fail("Could not create account in time limit");
	}
	
	private static void blockingDeleteAccount(String iban) throws InterruptedException, UnknownAccountException {
		try {
			database.deleteAccount(iban);
		} catch (UnknownAccountException e) {
			return;
		}
		
		long start = System.currentTimeMillis();
		while (System.currentTimeMillis() - start < TIMEOUT) {
			try {
				Thread.sleep(SLEEP_TIME);
			} catch (InterruptedException e) {}
			try {
				database.getBalance(iban);
			} catch (UnknownAccountException e) {
				return;
			}
		}
		
		fail("Could not delete account in time limit");
	}
	
	@Test
	public void testGetBalance() throws UnknownAccountException {
		TestUtilities.assertEquals("Get balance returned wrong result for account: " + IBAN1, BALANCE1, database.getBalance(IBAN1));
		TestUtilities.assertEquals("Get balance returned wrong result for account: " + IBAN2, BALANCE2, database.getBalance(IBAN2));
	}
	
	@Test
	public void testGetBalanceUnknownAccount() throws UnknownAccountException {
		String iban = UNKNOWN;
		TestUtilities.expectUnknownAccountException(this.thrown, iban, BIC);
		database.getBalance(iban);
	}

	@Test
	public void testWithdraw() throws UnknownAccountException, AccountOverdrawException {
		database.withdraw(IBAN1, 200);
		TestUtilities.assertEquals("Withdraw failed for account: " + IBAN1, BALANCE1 - 200, database.getBalance(IBAN1));
		
		database.withdraw(IBAN2, 42);
		TestUtilities.assertEquals("Withdraw failed for account: " + IBAN2, BALANCE2 - 42, database.getBalance(IBAN2));
	}
	
	@Test
	public void testWithdrawUnknownAccount() throws UnknownAccountException, AccountOverdrawException {
		TestUtilities.expectUnknownAccountException(this.thrown, UNKNOWN, BIC);
		database.withdraw(UNKNOWN, 1);
	}
	
	@Test
	public void testWithdrawAccountOverdraw() throws UnknownAccountException, AccountOverdrawException {
		TestUtilities.expectException(this.thrown, AccountOverdrawException.class, IBAN1);
		database.withdraw(IBAN1, BALANCE1+1);
	}
	
	@Test
	public void testDeposit() throws UnknownAccountException {
		database.deposit(IBAN1, 100);
		TestUtilities.assertEquals("Deposit failed for account: " + IBAN1, BALANCE1 + 100, database.getBalance(IBAN1));
		
		database.deposit(IBAN2, 300);
		TestUtilities.assertEquals("Deposit failed for account: " + IBAN2, BALANCE2 + 300, database.getBalance(IBAN2));
	}
	
	@Test
	public void testDepositUnknownAccount() throws UnknownAccountException {
		TestUtilities.expectUnknownAccountException(this.thrown, UNKNOWN, BIC);
		database.deposit(UNKNOWN, 1);
	}
	
	@Test
	public void testAddAccount() throws KnownAccountException, UnknownAccountException, InterruptedException {
		String iban = "iban42";
		double balance = 42;
		blockingAddAccount(iban, balance);	
		TestUtilities.assertEquals("newly created account has wrong balance", balance, database.getBalance(iban));
	}
	
	@Test
	public void testAddAccountKnownException() throws KnownAccountException, UnknownAccountException {
		TestUtilities.expectException(this.thrown, KnownAccountException.class, IBAN1);
		database.addAccount(IBAN1, 1);
	}
	
	@Test
	public void testDeleteAccount() throws UnknownAccountException, KnownAccountException, InterruptedException {
		String iban = "iban42";
		blockingAddAccount(iban, 42);
		blockingDeleteAccount(iban);
		
		TestUtilities.expectUnknownAccountException(this.thrown, iban, BIC);
		database.getBalance(iban);
	}
	
	@Test
	public void testDeleteAccountUnknownAccount() throws UnknownAccountException {
		TestUtilities.expectUnknownAccountException(this.thrown, UNKNOWN, BIC);
		database.deleteAccount(UNKNOWN);
	}
	
	@Test
	public void testListAccounts() throws KnownAccountException, InterruptedException {
		Set<String> set1 = database.listAccounts();
		assertEquals("listAccount returned a wrong number of results: " + set1, 2, set1.size());
		assertTrue("listAccount did not contain account " + IBAN1, set1.contains(IBAN1));
		assertTrue("listAccount did not contain account " + IBAN2, set1.contains(IBAN2));
		
		String iban = "iban1337";
		blockingAddAccount(iban, 1337);
		
		Set<String> set2 = database.listAccounts();
		assertEquals("listAccount returned a wrong number of results: " + set2, 3, set2.size());
		assertTrue("listAccount did not contain account " + IBAN1, set2.contains(IBAN1));
		assertTrue("listAccount did not contain account " + IBAN2, set2.contains(IBAN2));
		assertTrue("listAccount did not contain account iban1337", set2.contains(iban));
	}
	
	@Test
	@Ignore("Bonus exercise")
	public void testPersistence() throws KnownAccountException, UnknownAccountException, AccountOverdrawException, InterruptedException, FileNotFoundException, IOException {
		blockingAddAccount("iban42", 42);
		database.withdraw(IBAN1, 300);
		database.deposit(IBAN2, 100);
		Set<String> accounts = database.listAccounts();
		
		database = null;
		Thread.sleep(1);		
		database = getDatabase();
		try {
			TestUtilities.assertEquals("Account " + IBAN1 + " has wrong balance", BALANCE1-300, database.getBalance(IBAN1));
			TestUtilities.assertEquals("Account " + IBAN2 + " has wrong balance", BALANCE2+100, database.getBalance(IBAN2));		
			TestUtilities.assertEquals("newly created account has not correct balance", 42, database.getBalance("iban42"));
		} catch (UnknownAccountException e) {
			fail("Failed to persist the account " + e.getIban());
		}
		
		
		assertEquals("Not the same list of accounts", accounts, database.listAccounts());
	}
}
