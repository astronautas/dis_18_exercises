package ch.unibas.dmi.dbis.dis.mom.test;

import static org.junit.Assert.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import org.junit.*;
import org.junit.rules.ExpectedException;

import ch.unibas.dmi.dbis.dis.mom.db.ITransactionTable;
import ch.unibas.dmi.dbis.dis.mom.exception.TransactionExistsException;
import ch.unibas.dmi.dbis.dis.mom.exception.UnknownTransactionException;
import ch.unibas.dmi.dbis.dis.mom.server.BankServer;
import ch.unibas.dmi.dbis.dis.mom.server.Transaction;

/**
 * JUnitTest that tests the functionality of the given implementation of {@link ITransactionTable}
 * provided by {@link BankServer#createTransactionTable(String, com.amazonaws.auth.AWSCredentials) createTransactionTable}
 * from {@link BankServer}.
 * This test does also check the persistence of the implementation.
 * 
 * @author Lukas Beck, HS2012
 */
public class TransactionTableTest {	
	private static final Transaction TX = new Transaction("iban", 1234);
	private static final String BIC = "transactionTest";
	private static final int TIMEOUT = 10*1000;
	private static final int SLEEP_TIME = 125;
	
	private static ITransactionTable txTable;
	
	@Rule
	public ExpectedException thrown = ExpectedException.none();
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		txTable = getTransactionTable();
		deleteAllTransactions();
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
		deleteAllTransactions();
	}
	
	public static ITransactionTable getTransactionTable() throws FileNotFoundException, IOException {
		//return new SimpleTransactionTable(BIC);
		return BankServer.createTransactionTable(BIC, BankServer.createCredentials());
	}
	
	public static void deleteAllTransactions() throws UnknownTransactionException {
		for (Entry<String, Transaction> e : txTable.list()) {
			txTable.remove(e.getKey());
		}
	}

	private static String blockingPut(Transaction tx) {
		String id = txTable.put(tx);
		waitUntilInserted(id, tx);
		return id;
	}
	
	private static void blockingPut(String id, Transaction tx) throws TransactionExistsException {
		txTable.put(id, tx);
		waitUntilInserted(id, tx);
	}

	public static void waitUntilInserted(String id, Transaction tx) {
		long start = System.currentTimeMillis();
		while (System.currentTimeMillis() - start < TIMEOUT) {
			try {
				Thread.sleep(SLEEP_TIME);
			} catch (InterruptedException e) {}
			
			try {
				Transaction newTx = txTable.get(id);
				assertEquals("newly created account has wrong transaction", tx, newTx);
				return;
			} catch (UnknownTransactionException e) {}
		}
		
		fail("Could not insert transaction in time limit");
	}
	
	private static void blockingRemove(String id) throws UnknownTransactionException {
		txTable.remove(id);
		long start = System.currentTimeMillis();
		while (System.currentTimeMillis() - start < TIMEOUT) {
			try {
				Thread.sleep(SLEEP_TIME);
			} catch (InterruptedException e) {}
			try {
				txTable.get(id);
			} catch (UnknownTransactionException e) {
				return;
			}
		}
		
		fail("Could not remove transaction in time limit");
	}
	
	private Map<String, Transaction> createSamples() {
		Map<String, Transaction> transactions = new HashMap<String, Transaction>();
		transactions.put("some old id", TX);
		transactions.put("another id with $$$", new Transaction("the universe's money", 42));
		transactions.put("5", new Transaction("another iban", 12345));
		return transactions;
	}
	
	private void insertMap(Map<String, Transaction> transactions) throws TransactionExistsException {
		for (Entry<String, Transaction> e : transactions.entrySet()) {
			blockingPut(e.getKey(), e.getValue());
		}
	}
	
	@Test
	public void testPut() throws TransactionExistsException, UnknownTransactionException {
		String id = "an id";	
		blockingPut(id, TX);
		assertTrue("Transaction did not get inserted", txTable.containsId(id));
		assertEquals("Received transaction is not the same as the inserted", TX, txTable.get(id));
	}
	
	@Test
	public void testPutTransactionExists() throws TransactionExistsException {
		String id = blockingPut(TX);
		assertTrue("Transaction did not get inserted", txTable.containsId(id));
		
		TestUtilities.expectException(thrown, TransactionExistsException.class);
		thrown.expectMessage(id);
		thrown.expectMessage(TX.toString());
		
		txTable.put(id, TX);
	}
	
	@Test
	public void testPutGenerated() throws UnknownTransactionException {
		String id = blockingPut(TX);
		assertTrue("Transaction did not get inserted", txTable.containsId(id));
		assertEquals("Received transaction is not the same as the inserted", TX, txTable.get(id));
	}
	
	@Test
	public void testGetUnknownTransaction() throws UnknownTransactionException {
		String id = "unknown id";
		
		assertFalse("Transaction exists even though it should not", txTable.containsId(id));
		TestUtilities.expectException(thrown, UnknownTransactionException.class, id);
		txTable.get(id);
	}
	
	@Test
	public void testRemove() throws UnknownTransactionException {
		String id = blockingPut(TX);
		assertTrue("Transaction did not get inserted", txTable.containsId(id));
		blockingRemove(id);
		assertFalse("Transaction did not get removed", txTable.containsId(id));
		TestUtilities.expectException(thrown, UnknownTransactionException.class, id);
		txTable.get(id);
	}
	
	@Test
	public void testList() throws TransactionExistsException {
		Map<String, Transaction> transactions = createSamples();
		insertMap(transactions);
		
		Set<Entry<String, Transaction>> inserted = txTable.list();
		assertEquals("Listed and inserted transactions have not the same size", transactions.size(), inserted.size());
		assertEquals("Listed and inserted transactions are not the same", transactions.entrySet(), inserted);
	}
		
	@Test
	@Ignore("Bonus points")
	public void testPersistence() throws TransactionExistsException, InterruptedException, FileNotFoundException, IOException, UnknownTransactionException {
		Map<String, Transaction> samples = createSamples();
		insertMap(samples);
		
		txTable = null;
		Thread.sleep(1);
		txTable = getTransactionTable();
		
		assertEquals("Inserted transactions are not persistent", samples.entrySet(), txTable.list());
		
		List<String> keyList = new ArrayList<String>(samples.keySet());
		String id = keyList.get(new Random().nextInt(keyList.size()));
		samples.remove(id);
		txTable.remove(id);		
		
		txTable = null;
		Thread.sleep(1);
		txTable = getTransactionTable();
		
		assertEquals("Removal of transaction was not persistent", samples.entrySet(), txTable.list());
	}
}
