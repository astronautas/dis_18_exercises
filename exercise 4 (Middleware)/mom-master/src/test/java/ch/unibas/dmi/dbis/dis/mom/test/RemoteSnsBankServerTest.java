package ch.unibas.dmi.dbis.dis.mom.test;

import ch.unibas.dmi.dbis.dis.mom.db.IDatabase;
import ch.unibas.dmi.dbis.dis.mom.exception.*;
import ch.unibas.dmi.dbis.dis.mom.server.BankServer;
import ch.unibas.dmi.dbis.dis.mom.server.SnsBankServer;
import org.junit.*;
import org.junit.rules.ExpectedException;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * This JUnit-Test tests the {@link BankServer} functionality while using an remote bank server.
 * For testing the functionality only locally, use {@link LocalBankServerTest} instead.<br>
 * <br>
 * In order to correctly run this test, an already running instance of {@link BankServer} is needed.
 * Ideally, the running instance runs on Amazon EC2. To start a bank server on an EC2 instance,
 * just call the {@link BankServer#main(String[]) main} function of {@link BankServer} with the arguments
 * "-i", "remote_sns", "local_sns". This starts the server with the correct bank identifiers and
 * inserts hardcoded test data if not already present.<br>
 * <br>
 * Notice that this test needs the functionality of
 * {@link BankServer#getBalance(String, String) getBalance} and {@link IDatabase#listAccounts()}
 * in order to work correctly.<br>
 * Notice also that this test only tests remote functionality.
 * In order to test local aspects of the functionality, use {@link LocalBankServerTest}.
 * 
 * @author Lukas Beck, HS 2012
 */
public class RemoteSnsBankServerTest {
	private static final String LOCAL_BIC = "local_sns";
	private static final String REMOTE_BIC = "remote_sns";
	private static SnsBankServer server;
	private static String localIban1;
	private static String localIban2;
	private static String remoteIban1;
	private static String remoteIban2;
	
	
	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Explicitly do not delete the queues when interrupting and thus stopping the threads
		server = new SnsBankServer(LOCAL_BIC, REMOTE_BIC, true, false);
		String[] accounts = TestUtilities.insertTestData(server);
		localIban1 = accounts[0];
		localIban2 = accounts[1];
		server.start();
		
		// Use hardcoded identifiers
		remoteIban1 = "iban1";
		remoteIban2 = "iban2";
		// Check if the accounts exist
		try {
			server.getBalance(REMOTE_BIC, remoteIban1);		
			server.getBalance(REMOTE_BIC, remoteIban2);
		} catch (TransactionExpiredException e) {
			Assert.fail("Transaction expired - make sure that the remote server is running correctly.");
		}
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		// Switch the deleting of the queues on
		server.setDeleteQueuesAfterwards(true);
		server.interrupt();
	}
	
	@Test
	public void testTransferRemote() throws KnownAccountException, UnknownAccountException, IllegalOperationException, AccountOverdrawException, UnknownBicException, TransactionExpiredException, UnknownTransactionException, InterruptedException {
		String fromIban = localIban1;
		double fromBalance = server.getLocalBalance(fromIban);
		double amount = 1000;
		
		String toIban = remoteIban1;
		double toBalance = server.getBalance(REMOTE_BIC, toIban);		
	
		// Transfer from server1 to server2			
		server.transfer(REMOTE_BIC, fromIban, toIban, amount);
		
		// Wait until messages get processed in SQS, depends on your settings
		TestUtilities.sleepQuiet(9000);
		
		// Check new balances
		TestUtilities.assertEquals("After remote transfer (amount = "+amount+"), local account has wrong balance",
				fromBalance - amount, server.getLocalBalance(fromIban));
		TestUtilities.assertEquals("After remote transfer (amount = "+amount+"), remote account has wrong balance",
				toBalance + amount, server.getBalance(REMOTE_BIC, toIban));
	}
	
	@Test
	public void testTransferRemoteUnknownAccount() throws UnknownAccountException, IllegalOperationException, AccountOverdrawException, UnknownBicException {
		String fromIban = localIban2;
		double fromBalance = server.getLocalBalance(fromIban);
		double amount = ((int) fromBalance/2);
		try {
			server.transfer(REMOTE_BIC, fromIban, "unknown_account", amount);			
		} catch (UnknownAccountException e) {
			// This can happen, if someone implements a remote UnknownAccountException 
		} finally {
			// Wait until messages get processed in SQS, depends on your settings
			TestUtilities.sleepQuiet();
			TestUtilities.assertEquals("When transfering to remote unknown account, local balance changed",
					fromBalance, server.getLocalBalance(fromIban));
		}
	}
	
	@Test
	public void testTransferRemoteAccountOverdraw() throws UnknownAccountException, IllegalOperationException, UnknownBicException, TransactionExpiredException, UnknownTransactionException, InterruptedException {
		String fromIban = localIban2;
		double fromBalance = server.getLocalBalance(fromIban);
		double amount = fromBalance + 1;
		
		String toIban = remoteIban2;
		double toBalance = server.getBalance(REMOTE_BIC, toIban);
		
		try {
			server.transfer(REMOTE_BIC, fromIban, toIban, amount);
		} catch (AccountOverdrawException e) {
			// This can happen, if someone implements a remote AccountOverdrawException
		} finally {
			// Wait until messages get processed in SQS, depends on your settings
			TestUtilities.sleepQuiet();
			TestUtilities.assertEquals("When trying to transfer when an AccountOverdrawException happens, local balanced changed",
					fromBalance, server.getLocalBalance(fromIban));
			TestUtilities.assertEquals("When trying to transfer when an AccountOverdrawException happens, remote balanced changed",
					toBalance, server.getBalance(REMOTE_BIC, toIban));
		}
	}
	
	/**
	 * This test tests the persistence of the bank server.
	 * If your implementation does not support this feature yet,
	 * you have to comment out this test as it depends on the persistence of accounts.
	 */
	@SuppressWarnings("javadoc")
	@Test
    @Ignore("Bonus points")
	public void testTransferRemotePersistence() throws IllegalOperationException, UnknownAccountException, AccountOverdrawException, UnknownBicException, KnownAccountException, FileNotFoundException, IOException, TransactionExpiredException, UnknownTransactionException, InterruptedException {
		String fromIban = localIban1;
		double fromBalance = server.getLocalBalance(fromIban);
		double amount = ((int) fromBalance/2);
		
		String toIban = remoteIban2;
		double toBalance = server.getBalance(REMOTE_BIC, toIban);		
		
		// Transfer from server1 to server2			
		server.transfer(REMOTE_BIC, fromIban, toIban, amount);
		
		// Immediately restart server
		server.interrupt();
		// Explicitly do not clear or delete queues
		server = new SnsBankServer(LOCAL_BIC, REMOTE_BIC, false, false);
		server.start();
		
		// Wait until messages get processed in SQS, depends on your settings
		TestUtilities.sleepQuiet();
		
		// Check new balances
		TestUtilities.assertEquals("After remote transfer (amount = "+amount+") and restarting server, local account has wrong balance",
				fromBalance - amount, server.getLocalBalance(fromIban));
		TestUtilities.assertEquals("After remote transfer (amount = "+amount+"), remote account has wrong balance",
				toBalance + amount, server.getBalance(REMOTE_BIC, toIban));
	}
	
	/**
	 * This test tests the persistence of the bank server.
	 * If your implementation does not support this feature yet,
	 * you have to comment out this test as it depends on the persistence of accounts.
	 */
	@SuppressWarnings("javadoc")
	@Test
    @Ignore("Bonus points")
	public void testTransferRemotePersistenceAccountOverdraw() throws UnknownAccountException, IllegalOperationException, UnknownBicException, FileNotFoundException, IOException, TransactionExpiredException, UnknownTransactionException, InterruptedException {
		String fromIban = localIban2;
		double fromBalance = server.getLocalBalance(fromIban);
		double amount = fromBalance + 1;
		
		String toIban = remoteIban2;
		double toBalance = server.getBalance(REMOTE_BIC, toIban);
		
		try {
			server.transfer(REMOTE_BIC, fromIban, toIban, amount);
		} catch (AccountOverdrawException e) {
			// This can happen, if someone implements a remote AccountOverdrawException
		} finally {
			// Immediately restart server
			server.interrupt();
			server = new SnsBankServer(LOCAL_BIC, REMOTE_BIC, false, false);
			server.start();
			
			// Wait until messages get processed in SQS, depends on your settings
			TestUtilities.sleepQuiet();
			
			TestUtilities.assertEquals("When trying to transfer when an AccountOverdrawException happens and restarting server, local balanced changed",
					fromBalance, server.getLocalBalance(fromIban));
			TestUtilities.assertEquals("When trying to transfer when an AccountOverdrawException happens, remote balanced changed",
					toBalance, server.getBalance(REMOTE_BIC, toIban));
		}
	}

}
