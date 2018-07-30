package ch.unibas.dmi.dbis.dis.mom.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import ch.unibas.dmi.dbis.dis.mom.db.IDatabase;
import ch.unibas.dmi.dbis.dis.mom.db.ITransactionTable;
import ch.unibas.dmi.dbis.dis.mom.db.SimpleDatabase;
import ch.unibas.dmi.dbis.dis.mom.db.SimpleTransactionTable;
import ch.unibas.dmi.dbis.dis.mom.db.TransactionTable;
import ch.unibas.dmi.dbis.dis.mom.exception.AccountOverdrawException;
import ch.unibas.dmi.dbis.dis.mom.exception.IllegalOperationException;
import ch.unibas.dmi.dbis.dis.mom.exception.TransactionExpiredException;
import ch.unibas.dmi.dbis.dis.mom.exception.UnknownAccountException;
import ch.unibas.dmi.dbis.dis.mom.exception.UnknownBicException;
import ch.unibas.dmi.dbis.dis.mom.exception.UnknownTransactionException;
import ch.unibas.dmi.dbis.dis.mom.message.BalanceRequestMessage;
import ch.unibas.dmi.dbis.dis.mom.message.BalanceResultMessage;
import ch.unibas.dmi.dbis.dis.mom.message.BankMessage;
import ch.unibas.dmi.dbis.dis.mom.message.DepositRequestMessage;
import ch.unibas.dmi.dbis.dis.mom.message.DepositResultMessage;
import ch.unibas.dmi.dbis.dis.mom.message.RequestMessage;
import ch.unibas.dmi.dbis.dis.mom.queue.Queue;
import ch.unibas.dmi.dbis.dis.mom.test.TestUtilities;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;

/**
 * Message-oriented Bank Server based on Amazon SQS.
 * This bank server makes extensively use of {@link Queue},
 * {@link BankMessage} and their implementations, {@link IDatabase} and {@ITransactionTable}.
 * See the corresponding classes for more informations on how to use them.
 * 
 * @author Ilir Fetai, Filip-M. Brinkmann University of Basel, HS 2011, 2013
 * @author Lukas Beck, HS 2012
 */
public class BankServer extends Thread {
	protected static final String RESULT_PREFIX = "response_";
    protected static final String REQUEST_PREFIX = "request_";

    protected String bic;
    protected String remoteBic;

	// the Amazon SQS object and the credentials
    protected AmazonSQS sqs;
    protected AWSCredentials awsCredentials;
	
	// an interface to the local database instance
    protected IDatabase database;
	// The transaction table which maps transaction IDs to transactions.
    protected ITransactionTable transactionTable;

    protected static final int GET_BALANCE_EXPIRATION_TRY_INTERVAL = 6000;
    protected static final int GET_BALANCE_EXPIRATION_TRIES = 10;

    /**
	 * The bank server's own request queue. Other servers place requests in this queue.
	 */
	Queue<RequestMessage> myRequestQueue;
	/**
	 * The bank server's own response queue. Other servers place results of this server's requests in this queue.
	 */
	Queue<DepositResultMessage> myResponseQueue;
	/**
	 * The remote bank server's request queue. This server places requests in that queue.
	 */
	Queue<RequestMessage> remoteRequestQueue;
	/**
	 * The remote bank server's response queue. This server places results of the remote bank server's requests in that queue.
	 */
	Queue<DepositResultMessage> remoteResponseQueue;

	/**
	 * If true, the queues of this bank server get deleted when stopping ({@link #interrupt()}) the bank server.
	 */
	protected boolean deleteQueuesAfterwards;
	
	/**
	 * Creates a new BankServer and initializes Amazon SQS, the queues and the database.
	 * Notice that this constructor clears the queues that this bank server owns on startup and deletes them on shutdown.
	 * 
	 * <br>
	 * For explicitly not clearing the queues, use {@link #BankServer(String, String, boolean)}.<br>
	 * For explicitly not deleting the queues, use {@link #BankServer(String, String, boolean, boolean)}.
	 * 
	 * @param bic identifier of this bank server
	 * @param remoteBic identifier of the remote bank server
	 * @throws FileNotFoundException If the AWS credentials file does not exist
	 * @throws IOException If any problems occur while reading the AWS credentials file
	 */
	public BankServer(String bic, String remoteBic) throws FileNotFoundException, IOException {
		this(bic, remoteBic, true);
	}

    public BankServer() {

    }
	
	/**
	 * Creates a new BankServer and initializes Amazon SQS, the queues and the database. 
	 * Notice that this constructor deletes the queues that this bank server owns on shutdown.
	 * <br>
	 * For explicitly not deleting the queues, use {@link #BankServer(String, String, boolean, boolean)}.
	 * 
	 * @param bic identifier of this bank server
	 * @param remoteBic identifier of the remote bank server
	 * @param clearQueues if true, the queues that the bank server owns are cleared
	 * @throws FileNotFoundException If the AWS credentials file does not exist
	 * @throws IOException If any problems occur while reading the AWS credentials file
	 */
	public BankServer(String bic, String remoteBic, boolean clearQueues) throws FileNotFoundException, IOException {
		this(bic, remoteBic, clearQueues, true);
	}
	
	/**
	 * Creates a new BankServer and initializes Amazon SQS, the queues and the database. 
	 * 
	 * @param bic identifier of this bank server
	 * @param remoteBic identifier of the remote bank server
	 * @param clearQueues if true, the queues that the bank server owns are cleared
	 * @param deleteQueuesAfterwards if true, the queues get deleted after interrupting the bank server
	 * @throws FileNotFoundException If the AWS credentials file does not exist
	 * @throws IOException If any problems occur while reading the AWS credentials file
	 */
	public BankServer(String bic, String remoteBic, boolean clearQueues, boolean deleteQueuesAfterwards) throws FileNotFoundException, IOException {
		this.bic = bic;
		this.remoteBic = remoteBic;
		this.deleteQueuesAfterwards = deleteQueuesAfterwards;
		
		this.transactionTable = new SimpleTransactionTable(bic);
		
		this.awsCredentials = createCredentials(); 
		this.sqs = createSQS(this.awsCredentials);
		this.initializeQueues(clearQueues);
				
		this.database = createDatabase(this.bic, this.awsCredentials);
		this.transactionTable = createTransactionTable(this.bic, this.awsCredentials);
	}
	
	/**
	 * @return file of the AWS credentials file
	 */
	public static File getCredentialsFile() {
		return new File("AwsCredentials.properties");
	}
	
	/**
	 * This method returns the underlying database that the bank server should use.
	 * The object returned by this method is tested by DatabaseTest.
	 * You should adjust this method to return your own persistence database instance.
	 * 
	 * @param bic bank identifier of the bank server
	 * @param awsCredentials AWS credentials
	 * @return database database instance to use for the bank server
	 */
	public static IDatabase createDatabase(String bic, AWSCredentials awsCredentials) {
		//TODO: change to your own implementation, below is a simple one
		return new SimpleDatabase(bic);
	}
	
	/**
	 * This method returns the underlying database that the bank server should use.
	 * The object returned by this method is tested by TransactionTableTest.
	 * You should adjust this method to return your own persistence transaction table instance.
	 * 
	 * @param bic bank identifier of the bank server
	 * @param awsCredentials AWS credentials
	 * @return database persistence transaction table instance to use for the bank server
	 */
	public static ITransactionTable createTransactionTable(String bic, AWSCredentials awsCredentials) {
		
		//TODO: change to your own implementation, below is a simple one
		return new SimpleTransactionTable(bic);		
	}
	
	/**
	 * Creates and returns the AWS credentials object by using the file
	 * returned from {@link #getCredentialsFile()}.
	 * 
	 * @return AWS credentials object
	 * @throws FileNotFoundException If the file returned from {@link #getCredentialsFile()} cannot be found 
	 * @throws IOException If an I/O error occurs while reading the file
	 */
	public static AWSCredentials createCredentials() throws FileNotFoundException, IOException {
		InputStream is = new FileInputStream(getCredentialsFile());
		try {
			return new PropertiesCredentials(is);
		} finally {
			try {
				is.close();
			} catch (IOException e) {}
		}
	}
	
	/** 
	 * Creates the Amazon SQS object from the credentials object and
	 * sets the end point to the central European region.
	 *  
	 * @param awsCredentials AWS credentials
	 * @return the newly created Amazon SQS object
	 * 
	 * @throws FileNotFoundException If the AWS credentials file does not exist
	 * @throws IOException If any problems occur while reading the AWS credentials file
	 */
	public static AmazonSQS createSQS(AWSCredentials awsCredentials) throws FileNotFoundException, IOException {
		System.out.print("Instantiating SQS client...");
		AmazonSQS sqs = new AmazonSQSClient(awsCredentials);
		System.out.println(" Done!");
		
		// set it to the central European region end point. Not necessary, but closer to Switzerland :)
		sqs.setEndpoint("sqs.eu-central-1.amazonaws.com");
		return sqs;
	}
	
	/**
	 * Sets up the appropriate queues and clears their content.
	 */
    void initializeQueues(boolean clearQueues) {
		System.out.print("Initializing queues...");
		myRequestQueue = new Queue<RequestMessage>(sqs, REQUEST_PREFIX + bic);
		myResponseQueue = new Queue<DepositResultMessage>(sqs, RESULT_PREFIX + bic);
		remoteRequestQueue = new Queue<RequestMessage>(sqs, REQUEST_PREFIX + remoteBic);
		remoteResponseQueue = new Queue<DepositResultMessage>(sqs, RESULT_PREFIX + remoteBic);
		System.out.println(" Done!");
		
		if (clearQueues) {
			System.out.print("Deleting all remaining messages...");
			myRequestQueue.deleteAllMessages();
			myResponseQueue.deleteAllMessages();		
			System.out.println(" Done!");
		}
	}

	public IDatabase getDatabase() {
		return this.database;
	}
	
	public void setDeleteQueuesAfterwards(boolean deleteQueuesAfterwards) {
		this.deleteQueuesAfterwards = deleteQueuesAfterwards;
	}
	
	/**
	 * The main worker method of the server.
	 * It polls messages out of its request and response queue
	 * and handles the messages accordingly.
	 */
	@Override
	public void run() {
		System.out.println("Starting bank server with BIC '"+this.bic+"'.");

		while (true) {
			// Check and handle new request
			RequestMessage requestMessage = myRequestQueue.getMessage();

			if (requestMessage != null) {				
				handleRequest(requestMessage);
				myRequestQueue.deleteMessage(requestMessage);
			}
			
			// Check and handle all results
			List<DepositResultMessage> resultMessages = myResponseQueue.getMessages();
			handleResults(resultMessages);
			myResponseQueue.deleteMessages(resultMessages);
			
			// Check for expired transactions
			checkAndCompensateExpiredTransactions();
			
			try {
				Thread.sleep(250);
			} catch (InterruptedException e) {
				// Stop if it has been interrupted
				break;
			}
		}
		
		System.out.print("Stopping bank server");		
		if (deleteQueuesAfterwards) {
			System.out.print(" and deleting our queues...");
			try {
				myRequestQueue.close();
			} catch (IOException e) {}
			try {
				myResponseQueue.close();
			} catch (IOException e) {}
		}
		else {
			System.out.print("...");
		}
		System.out.println(" Done");
	}
	
	/**
	 * Delegates the handling of the request message to either {@link #handleBalanceRequest(BalanceRequestMessage)}
	 * or {@link #handleDepositRequest(DepositRequestMessage)}.
	 * Gives out a warning, if the request message is of an unknown type.
	 * 
	 * @param requestMessage to be handled request message
	 */
	protected void handleRequest(RequestMessage requestMessage) {
		if (requestMessage instanceof BalanceRequestMessage) {
			handleBalanceRequest((BalanceRequestMessage) requestMessage);
		}
		else if (requestMessage instanceof DepositRequestMessage) {			
			handleDepositRequest((DepositRequestMessage) requestMessage);
		}
		else {
			System.err.println("Warning: recieved unknown request message: " + requestMessage.getClass() + ", ignoring it.");
		}
	}

	/**
	 * Handles a deposit request by doing a local deposit and
	 * sends the result back.
	 * It sends an okay if and only if we were the target bank and the deposit was successful.
	 * 
	 * @param depositRequest to be handled deposit request
	 */
	public void handleDepositRequest(DepositRequestMessage depositRequest) {
		boolean success = false;
		
		// Check if we are the correct bank
		if (this.bic.equals(depositRequest.getBic())) {
			try {
				this.localDeposit(depositRequest.getIban(), depositRequest.getAmount());
				success = true; // success is true iff we are the target bank and deposit was successful 
			} catch (Exception e) {
				// deposit failed
			}
		}
		else {
			// Wrong bank
		}
		
		// Sends the result back
		DepositResultMessage result = new DepositResultMessage(depositRequest.getTransactionId(), success);
		remoteResponseQueue.sendMessage(result);
	}

	/**
	 * Handles a getBalance request and sends the result to the temporary queue.
	 * 
	 * @param balanceRequest to be handled getBalance request
	 */
	public void handleBalanceRequest(BalanceRequestMessage balanceRequest) {
		String txId = balanceRequest.getTransactionId();
		String iban = balanceRequest.getIban();
		
		BalanceResultMessage result;
		try {
			double balance = this.getLocalBalance(iban);
			result = new BalanceResultMessage(txId, balance);
		} catch (UnknownAccountException e) {
			result = new BalanceResultMessage(txId, e);
		}
		
		// Send message to temporary queue
		// We don't close it here, because we don't own it
		@SuppressWarnings("resource")
		Queue<BalanceResultMessage> tempQueue = new Queue<>(sqs, txId);
		tempQueue.sendMessage(result);
	}

	/**
	 * Handles all results of deposit requests by compensating if a deposit failed.
	 * Prints a warning if there is a result of a unknown transaction
	 * 
	 * @param resultMessages to be handled result messages
	 */
	private void handleResults(List<DepositResultMessage> resultMessages) {
		for (DepositResultMessage msg : resultMessages) {
			String txId = msg.getTransactionId();
			
			if (!transactionTable.containsId(txId)) {
				System.err.println("Warning: Received result of unknown transaction: " + txId
						+ ", contents of table: " + transactionTable);
				continue;
			}
			
			// If deposit failed, compensate it
			if (!msg.hasSucceded()) {
				try {
					compensate(transactionTable.get(txId));
				} catch (UnknownTransactionException e) {
					System.err.println("Warning: could not find account while trying to compensate a failed transfer...");
				}
			}

			try {
				transactionTable.remove(txId);
			} catch (UnknownTransactionException e) {
				System.err.println("Warning: could not remove transaction id '" + txId + "' from table");
			}
		}
	}

	/**
	 * Checks for any expired transactions which are compensated
	 * and removed from the table.
	 */
	protected void checkAndCompensateExpiredTransactions() {
		synchronized (transactionTable) {
			// Check for each transaction
			for (Entry<String, Transaction> e : transactionTable.list()) {
				String id = e.getKey();
				Transaction trx = e.getValue();

				// Check if transaction is expired
				if (TransactionTable.isTransactionExpired(trx)) {
					this.compensate(trx);
					try {
						transactionTable.remove(id);
					} catch (UnknownTransactionException e1) {}
				}
			}
		}
	}
	
	/**
	 * Compensates a failed transaction by doing a local deposit.
	 *
	 * @param trx the transaction
	 */
	protected void compensate(Transaction trx) {
		if (trx.amount == 0) {
			// if amount is zero, nothing to do
			return;
		}
		
		try {
			localDeposit(trx.iban, trx.amount);
		} catch (UnknownAccountException e) {
			// Should never happen, because we already successfully executed withdraw
			System.err.println("Warning: trying to compensate a failed transaction on an unknown account.");
		} catch (IllegalOperationException e) {
			// Should never happen, because we already successfully executed withdraw
			System.err.println("Warning: trying to compensate a failed transaction with a negative amount.");
		}
	}
	
	
	/**
	 * Transfers the given amount from the local bank account fromIban to the bank account toIban on bank toBic. 
	 * 
	 * @param toBic target bic
	 * @param fromIban local account iban
	 * @param toIban target account iban
	 * @param amount transfer amount
	 * @throws UnknownAccountException If local account is unknown
	 * @throws AccountOverdrawException If local account has insufficient funds
	 * @throws IllegalOperationException If the amount is negative or zero
	 * @throws UnknownBicException If {@code toBic} is unknown
	 */
	public synchronized void transfer(String toBic, String fromIban, String toIban, double amount)
			throws IllegalOperationException, UnknownAccountException, AccountOverdrawException, UnknownBicException {

		if (amount <= 0) {
			throw new IllegalOperationException("The amount to transfer must be > 0!");
		}

		String trId = transactionTable.put(new Transaction(fromIban, amount));
		withdraw(fromIban, amount);
		deposit(trId, toBic, toIban, amount);
	}
	
	/**
	 * Performs a local withdraw.
	 * 
	 * @param iban the account number
	 * @param amount the amount to withdraw
	 * 
	 * @return the new account balance
	 * 
	 * @throws IllegalOperationException if the amount is negative or zero
	 * @throws UnknownAccountException if the account is unknown
	 * @throws AccountOverdrawException if there is not enough money in the account
	 */
	public double withdraw(String iban, double amount) throws IllegalOperationException, UnknownAccountException, AccountOverdrawException {
		if (amount <= 0) {
			throw new IllegalOperationException("The amount to deposit must be positive");
		}
		
		// local withdraw		
		synchronized (database) {
			// check account balance first
			double balance = getLocalBalance(iban);
			if (balance >= amount) {
				// then withdraw
				this.database.withdraw(iban, amount);
			} else {
				throw new AccountOverdrawException(iban);
			}
		}

		return getLocalBalance(iban);
	}
	
	/**
	 * Performs a deposit.
	 * 
	 * @param trxId the transaction ID
	 * @param bic the bank to perform a deposit on
	 * @param iban the iban of the account
	 * @param amount the amount to deposit
	 * 
	 * @throws UnknownAccountException if the iban is unknown
	 * @throws UnknownBicException if the bic is unknown
	 * @throws IllegalOperationException if the amount is negative or zero
	 */
	public void deposit(String trxId, String bic, String iban, double amount) throws UnknownAccountException,
			UnknownBicException, IllegalOperationException {

		if (amount <= 0) {
			throw new IllegalOperationException("The amount to deposit must be positive");
		}

		if (!remoteBic.equals(bic) && !this.bic.equals(bic)) {
			throw new UnknownBicException(bic);
		}

		// Local transfer, non existing iban
		if (this.bic.equals(bic) && !this.database.listAccounts().contains(iban)) {
			throw new UnknownAccountException(bic, iban);
		}

		// Either local deposit or remote
		if (this.bic.equals(bic)) {
			localDeposit(iban, amount);
		} else {
			remoteRequestQueue.sendMessage(new DepositRequestMessage(trxId, bic, iban, amount));
		}
	}

	/**
	 * Deposits locally amount on the given account.
	 * 
	 * @param iban account iban
	 * @param amount amount to deposit
	 * @throws UnknownAccountException if the account does not exist
	 * @throws IllegalOperationException if the amount is negative or zero
	 */
	public void localDeposit(String iban, double amount) throws UnknownAccountException, IllegalOperationException {
		if (amount <= 0) {
			throw new IllegalOperationException("The amount to deposit must be positive");
		}
		
		synchronized (database) {
			this.database.deposit(iban, amount);
		}
	}
	
	/**
	 * Gets the balance of an account.
	 * 
	 * @param bic bank on which the account exists
	 * @param iban account number
	 * @return balance of the account
	 * @throws UnknownAccountException if the account number is unknown
	 * @throws UnknownBicException if the bank identifier code is unknown
	 * @throws TransactionExpiredException if the transaction expired while waiting for the result
	 */
	public double getBalance(String bic, String iban) throws UnknownAccountException, UnknownBicException, TransactionExpiredException, InterruptedException, UnknownTransactionException {
		if (!remoteBic.equals(bic) && !this.bic.equals(bic)) {
			throw new UnknownBicException(bic);
		}

		if (this.bic.equals(bic)) {
			return getLocalBalance(iban);
		} else {
			String trId = transactionTable.put(new Transaction(iban));
			remoteRequestQueue.sendMessage(new BalanceRequestMessage(trId, iban));

			// Poll temporary queue for the result
			Queue<BalanceResultMessage> tempQueue = new Queue<>(sqs, trId);

			int trExpirationTries = GET_BALANCE_EXPIRATION_TRIES;
			while (trExpirationTries != 0) {
				BalanceResultMessage balanceResponse = tempQueue.getMessage();

				if (balanceResponse != null) {
					try {
						tempQueue.close();
					} catch (IOException e) {
						e.printStackTrace();
					}

					return balanceResponse.getBalance();
				} else {
					trExpirationTries -= 1;
				}

				Thread.sleep(GET_BALANCE_EXPIRATION_TRY_INTERVAL);
			}

			throw new TransactionExpiredException(trId, transactionTable.get(trId));
		}
	}
	
	/**
	 * Gets the balance of a local account
	 * 
	 * @param iban account number
	 * @return balance of the account
	 * @throws UnknownAccountException if the account number is unknown
	 */
	public double getLocalBalance(String iban) throws UnknownAccountException {
		return this.database.getBalance(iban);
	}
	
	/**
	 * When started from commandline, the class takes two arguments: the local
	 * bic and the bic of the remote bank server.
	 * Additionally, it can take the option "-i" before the two arguments.
	 * If given, hardcoded test data is inserted into the bank server if not already present.
	 * Use this option if you want to run a bank server on an Amazon EC2 instance
	 * and test it with RemoteBankServerTest.
	 * 
	 * @param args [-i] &lt;bic&gt; &lt;remotebic&gt;
	 */
	public static void main(String[] args) {
		if (args.length < 2) {
			showHelp();
		}
		
		boolean insertTestData = false;
		if (args[0].equals("-i")) {
			insertTestData = true;
			args = shiftLeft(args, 1);
		}
		
		if (args.length < 2) {
			showHelp();
		}
		
		String bic = args[0];
		String remoteBic = args[1];
		
		BankServer server = null;
		try {	
			server = new BankServer(bic, remoteBic);			
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}
		
		if (insertTestData) {
			TestUtilities.insertTestData(server, true);
		}
		
		server.start();
	}

	static void showHelp() {
		System.err.println("Please specify bic and remote bic.");
		System.out.println("Usage: java BankServer [-i] <bic> <remotebic>");
		System.out.println("\tIf the option -i is given, then test data is inserted into" +
				"the database of the server if not already present.");
		System.exit(-1);
	}

	protected static String[] shiftLeft(String[] args, int i) {
		if (i >= args.length) {
			return null;
		}
		
		return Arrays.copyOfRange(args, i, args.length);
	}
}
