package ch.unibas.dmi.dbis.dis.mom.test;

import java.io.FileNotFoundException;
import java.io.IOException;

import ch.unibas.dmi.dbis.dis.mom.exception.*;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import ch.unibas.dmi.dbis.dis.mom.server.BankServer;

/**
 * This JUnit-Test tests the whole {@link BankServer} functionality locally.
 * In other words, it spawns two bank servers and tests their functionality.
 * For testing a running EC2 instance, use {@link RemoteBankServerTest}.
 * 
 * @author Lukas Beck, HS 2012
 * @author Filip-M. Brinkmann, HS 2013
 */
public class LocalBankServerTest {
	private static final String BIC1 = "test1";
	private static final String BIC2 = "test2";
	
	private static BankServer server1;
	private static BankServer server2;
	private static String iban11;
	private static String iban12;
	private static String iban21;
	private static String iban22;
	
	@Rule
	public ExpectedException thrown = ExpectedException.none();
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Explicitly do not delete the queues when interrupting and thus stopping the threads
		server1 = new BankServer(BIC1, BIC2, true, false);
		server2 = new BankServer(BIC2, BIC1, true, false);
		String[] accounts = TestUtilities.insertTestData(server1, server2);
		iban11 = accounts[0];
		iban12 = accounts[1];
		iban21 = accounts[2];
		iban22 = accounts[3];
		
		// Ensure that there is enough balance to test
		server1.localDeposit(iban11, 1000);
		server1.localDeposit(iban12, 1000);
		server2.localDeposit(iban21, 1000);
		server2.localDeposit(iban22, 1000);
		
		server1.start();
		server2.start();
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		// Switch the deleting of the queues on
		server1.setDeleteQueuesAfterwards(true);
		server2.setDeleteQueuesAfterwards(true);
		
		server1.interrupt();
		server2.interrupt();
	}

	@Test
	public void testWithdraw() throws UnknownAccountException, IllegalOperationException, AccountOverdrawException {
		double balance = server1.getLocalBalance(iban11);
		double amount = 500;
		if (balance < amount) {
			amount = balance;
		}
		TestUtilities.assertEquals("After withdraw, the account balance is wrong",
				balance - amount, server1.withdraw(iban11, amount));
	}

	@Test
	public void testLocalTransfer() throws UnknownAccountException, IllegalOperationException, AccountOverdrawException, UnknownBicException {
		double fromBalance = server2.getLocalBalance(iban21);
		double toBalance = server2.getLocalBalance(iban22);
		
		int amount = (int) (fromBalance/2);
		server2.transfer(BIC2, iban21, iban22, amount);
		TestUtilities.assertEquals("After transfering locally, fromBalance is wrong", fromBalance - amount, 
				server2.getLocalBalance(iban21));
		TestUtilities.assertEquals("After transfering locally, toBalance is wrong",   toBalance + amount,
				server2.getLocalBalance(iban22));
	}
	
	@Test
	public void testLocalTransferSameAccount() throws IllegalOperationException, UnknownAccountException, AccountOverdrawException, UnknownBicException {
		double balance = server1.getLocalBalance(iban12);
		server1.transfer(BIC1, iban12, iban12, 10);
		TestUtilities.assertEquals("After transfering to the same account, balance changed",
				balance, server1.getLocalBalance(iban12));
	}
	
	@Test
	public void testLocalTransferUnknownAccount() throws IllegalOperationException, UnknownAccountException, AccountOverdrawException, UnknownBicException {
		String iban = "unknown_iban";
		TestUtilities.expectUnknownAccountException(thrown, iban, BIC1);
		server1.transfer(BIC1, iban11, iban, 1);
	}

	@Test
	public void testLocalTransferAccountOverdraw() throws UnknownAccountException, IllegalOperationException, AccountOverdrawException, UnknownBicException {
		double balance = server2.getLocalBalance(iban22);
		TestUtilities.expectException(thrown, AccountOverdrawException.class, iban22);
		try {
			server2.transfer(BIC2, iban22, iban21, balance+1);
		} finally {
			TestUtilities.assertEquals("After a AccountOverdraw exception, the balance changed",
					balance, server2.getLocalBalance(iban22));
		}
	}
	
	@Test
	public void testTransferNegativeAmount() throws IllegalOperationException, UnknownAccountException, AccountOverdrawException, UnknownBicException {
		double balance = server1.getLocalBalance(iban12);
		TestUtilities.expectException(thrown, IllegalOperationException.class);
		try {
			server1.transfer(BIC2, iban12, iban21, -1);
		} finally {
			TestUtilities.assertEquals("When trying to transfer a negative amount, the balance changed",
					balance, server1.getLocalBalance(iban12));
		}
	}
	
	@Test
	public void testTransferZeroAmount() throws IllegalOperationException, UnknownAccountException, AccountOverdrawException, UnknownBicException {
		double balance = server2.getLocalBalance(iban21);
		TestUtilities.expectException(thrown, IllegalOperationException.class);
		try {
			server2.transfer(BIC1, iban21, iban12, 0);
		} finally {
			TestUtilities.assertEquals("When trying to transfer with a zero amount, the balance changed",
					balance, server2.getLocalBalance(iban21));
		}
	}
	
	@Test
	public void testTransferUnknownBic() throws IllegalOperationException, UnknownAccountException, AccountOverdrawException, UnknownBicException {
		String unknownBic = "unknown_bic";
		TestUtilities.expectException(thrown, UnknownBicException.class, unknownBic);
		server2.transfer(unknownBic, iban21, "toIban", 1);
	}
	
	@Test
	public void testTransferRemote() throws KnownAccountException, UnknownAccountException, IllegalOperationException, AccountOverdrawException, UnknownBicException {
		String fromIban = iban11;
		double fromBalance = server1.getLocalBalance(fromIban);
		double amount = ((int) fromBalance/2);
		
		// Insert an account into server2 to use for transfer
		// This avoids using the remote getBalance method (tested separately)
		String toIban = "new_iban";
		double toBalance = 12345;
		server2.getDatabase().addAccount(toIban, toBalance);		
		
		try {
			// Transfer from server1 to server2			
			server1.transfer(BIC2, fromIban, toIban, amount);
			
			// Wait until messages get processed in SQS, depends on your settings
			TestUtilities.sleepQuiet();
			
			// Check new balances
			TestUtilities.assertEquals("After remote transfer (amount = "+amount+"), local account has wrong balance",
					fromBalance - amount, server1.getLocalBalance(fromIban));
			TestUtilities.assertEquals("After remote transfer (amount = "+amount+"), remote account has wrong balance",
					toBalance + amount, server2.getLocalBalance(toIban));
		}
		finally {
			// Delete account
			server2.getDatabase().deleteAccount(toIban);
		}
	}
	
	@Test
	public void testTransferRemoteUnknownAccount() throws UnknownAccountException, IllegalOperationException, AccountOverdrawException, UnknownBicException {
		String fromIban = iban11;
		double fromBalance = server1.getLocalBalance(fromIban);
		double amount = ((int) fromBalance/2);
		try {
			server1.transfer(BIC2, fromIban, "unknown_account", amount);			
		} catch (UnknownAccountException e) {
			// This can happen, if someone implements a remote UnknownAccountException 
		} finally {
			// Wait until messages get processed in SQS, depends on your settings
			TestUtilities.sleepQuiet();
			TestUtilities.assertEquals("When transfering to remote unknown account, local balance changed",
					fromBalance, server1.getLocalBalance(fromIban));
		}
	}
	
	@Test
	public void testTransferRemoteAccountOverdraw() throws UnknownAccountException, IllegalOperationException, UnknownBicException {
		String fromIban = iban22;
		double fromBalance = server2.getLocalBalance(fromIban);
		double amount = fromBalance + 1;
		
		String toIban = iban12;
		double toBalance = server1.getLocalBalance(toIban);
		
		try {
			server2.transfer(BIC1, fromIban, toIban, amount);
		} catch (AccountOverdrawException e) {
			// This can happen, if someone implements a remote AccountOverdrawException
		} finally {
			// Wait until messages get processed in SQS, depends on your settings
			TestUtilities.sleepQuiet();
			TestUtilities.assertEquals("When trying to transfer when an AccountOverdrawException happens, local balanced changed",
					fromBalance, server2.getLocalBalance(fromIban));
			TestUtilities.assertEquals("When trying to transfer when an AccountOverdrawException happens, remote balanced changed",
					toBalance, server1.getLocalBalance(toIban));
		}
	}
	
	/**
	 * This test tests the persistence of the bank server.
	 * If your implementation does not support this feature yet,
	 * you have to comment out this test as it depends on the persistence of accounts.
	 */
	@SuppressWarnings("javadoc")
	// TODO: uncomment next line as soon as you have a persistent database 
	//@Test
	public void testTransferRemotePersistence() throws IllegalOperationException, UnknownAccountException, AccountOverdrawException, UnknownBicException, KnownAccountException, FileNotFoundException, IOException {
		String fromIban = iban12;
		double fromBalance = server1.getLocalBalance(fromIban);
		double amount = ((int) fromBalance/2);
		
		// Insert an account into server2 to use for transfer
		// This avoids using the remote getBalance method (tested separately)
		String toIban = "new_iban";
		double toBalance = 12345;
		server2.getDatabase().addAccount(toIban, toBalance);		
		
		try {
			// Transfer from server1 to server2			
			server1.transfer(BIC2, fromIban, toIban, amount);
			
			// Immediately restart server
			server1.interrupt();
			// Explicitly do not clear or delete queues
			server1 = new BankServer(BIC1, BIC2, false, false);
			server1.start();
			
			// Wait until messages get processed in SQS, depends on your settings
			TestUtilities.sleepQuiet();
			
			// Check new balances
			TestUtilities.assertEquals("After remote transfer (amount = "+amount+") and restarting server, local account has wrong balance",
					fromBalance - amount, server1.getLocalBalance(fromIban));
			TestUtilities.assertEquals("After remote transfer (amount = "+amount+"), remote account has wrong balance",
					toBalance + amount, server2.getLocalBalance(toIban));
		}
		finally {
			// Delete account
			server2.getDatabase().deleteAccount(toIban);
		}
	}
	
	/**
	 * This test tests the persistence of the bank server.
	 * If your implementation does not support this feature yet,
	 * you have to comment out this test as it depends on the persistence of accounts.
	 */
	@SuppressWarnings("javadoc")
	// TODO: uncomment next line as soon as you have a persistent database 
	//@Test
	public void testTransferRemotePersistenceAccountOverdraw() throws UnknownAccountException, IllegalOperationException, UnknownBicException, FileNotFoundException, IOException {
		String fromIban = iban22;
		double fromBalance = server2.getLocalBalance(fromIban);
		double amount = fromBalance + 1;
		
		String toIban = iban12;
		double toBalance = server1.getLocalBalance(toIban);
		
		try {
			server2.transfer(BIC1, fromIban, toIban, amount);
		} catch (AccountOverdrawException e) {
			// This can happen, if someone implements a remote AccountOverdrawException
		} finally {
			// Immediately restart server
			server2.interrupt();
			server2 = new BankServer(BIC2, BIC1, false, false);
			server2.start();
			
			// Wait until messages get processed in SQS, depends on your settings
			TestUtilities.sleepQuiet();
			
			TestUtilities.assertEquals("When trying to transfer when an AccountOverdrawException happens and restarting server, local balanced changed",
					fromBalance, server2.getLocalBalance(fromIban));
			TestUtilities.assertEquals("When trying to transfer when an AccountOverdrawException happens, remote balanced changed",
					toBalance, server1.getLocalBalance(toIban));
		}
	}
	
	@Test
	public void testGetBalanceLocal() throws UnknownAccountException, UnknownBicException, TransactionExpiredException, UnknownTransactionException, InterruptedException {
		
		TestUtilities.assertEquals("Get balance returned locally another result",
				server1.getLocalBalance(iban12), server1.getBalance(BIC1, iban12));
	}
	
	@Test
	public void testGetBalanceLocalUnknownAccount() throws UnknownAccountException, UnknownBicException, TransactionExpiredException, UnknownTransactionException, InterruptedException {
		String iban = "unknown_iban";
		TestUtilities.expectUnknownAccountException(thrown, iban, BIC1);
		server1.getBalance(BIC1, iban);
	}
	
	@Test
	public void testGetBalanceUnknownBic() throws UnknownAccountException, UnknownBicException, TransactionExpiredException, UnknownTransactionException, InterruptedException {
		String bic = "unknown_bic";
		TestUtilities.expectException(thrown, UnknownBicException.class, bic);
		server1.getBalance(bic, "unknown_iban");
	}
	
	@Test
	public void testGetBalanceRemote() throws KnownAccountException, UnknownAccountException, UnknownBicException, TransactionExpiredException, UnknownTransactionException, InterruptedException {
		// Insert an account into server2
		String iban = "new_iban";
		double balance = 12345;
		server2.getDatabase().addAccount(iban, balance);
		
		try {
			// Request getBalance from server1
			TestUtilities.assertEquals("Remote getBalance returned wrong result", balance, server1.getBalance(BIC2, iban));
		}
		finally {
			// Delete account
			server2.getDatabase().deleteAccount(iban);
		}		
	}
}
