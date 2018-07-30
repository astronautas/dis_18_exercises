package ch.unibas.dmi.dbis.dis.mom.test;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import ch.unibas.dmi.dbis.dis.mom.exception.UnknownAccountException;
import ch.unibas.dmi.dbis.dis.mom.message.BalanceRequestMessage;
import ch.unibas.dmi.dbis.dis.mom.message.BalanceResultMessage;
import ch.unibas.dmi.dbis.dis.mom.message.BankMessage;
import ch.unibas.dmi.dbis.dis.mom.message.DepositRequestMessage;
import ch.unibas.dmi.dbis.dis.mom.message.DepositResultMessage;
import ch.unibas.dmi.dbis.dis.mom.queue.Queue;
import ch.unibas.dmi.dbis.dis.mom.server.BankServer;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;

/**
 * This class tests the functionality of the queues.
 * For this tests, it is required that {@link BankServer#createCredentials() createCredentials}
 * and {@link BankServer#createSQS(AWSCredentials) createSQS} from {@link BankServer} work correctly.
 * 
 * @author Lukas Beck, HS 2012
 */
public class QueueTest {
	private static final int TIMEOUT = 30*1000;
	private static AmazonSQS sqs;
	private Queue<BankMessage> queue;
	
	@Rule
    public ExpectedException thrown = ExpectedException.none();
	
	@BeforeClass
	public static void setUpClass() throws Exception {
		AWSCredentials credentials = BankServer.createCredentials();
		sqs = BankServer.createSQS(credentials);
	}
	
	@AfterClass
	public static void tearDownClass() throws Exception {

	}
	
	@Before
	public void setUp() throws Exception {
		
	}

	@After
	public void tearDown() throws Exception {
		queue.close();
	}

	private void initQueue(String queueName) {
		queue = new Queue<BankMessage>(sqs, queueName);
		queue.deleteAllMessages();
	}
	
	private void checkQueueExistence(String queueName) throws QueueDoesNotExistException {
		try {
			sqs.getQueueUrl(new GetQueueUrlRequest(queueName));
		} catch (AmazonServiceException e) {
			if (e.getErrorCode().equals("AWS.SimpleQueueService.NonExistentQueue")) {
				throw new QueueDoesNotExistException(queueName);
			}
			
			throw e;
		}
	}
	
	private static boolean isTimeout(long startTime) {
		return System.currentTimeMillis() - startTime > TIMEOUT;
	}
	
	private static List<BankMessage> createMessageList() {
		List<BankMessage> sendList = new ArrayList<BankMessage>();
		sendList.add(new BalanceRequestMessage("txId2", "iban2"));
		sendList.add(new BalanceResultMessage("txId3", 42));
		sendList.add(new BalanceResultMessage("txId4", new UnknownAccountException("bic1", "iban3")));
		sendList.add(new DepositRequestMessage("txId5", "bic2", "iban4", 1337));
		sendList.add(new DepositResultMessage("txId6", true));
		return sendList;
	}
	
	@Test
	public void testCreationAndDeletion() throws IOException {
		String queueName = "testCreationAndDeletion";
		initQueue(queueName);
		
		// Get the url, this mustn't throw any exception
		// because the queue must exist
		checkQueueExistence(queueName);
		
		// Close and delete queue
		queue.close();
				
		// Wait for Amazon SQS to handle the delete request
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {}
				
		// Set the expected exception to QueueDoesNotExistException
		thrown.expect(QueueDoesNotExistException.class);
		thrown.expectMessage(queueName);
		
		checkQueueExistence(queueName);
	}
	
	@Test
	public void testSingleMessage() throws Exception {
		initQueue("testSingleMessage");
		
		BalanceRequestMessage sendMsg = new BalanceRequestMessage("txId", "iban");
		queue.sendMessage(sendMsg);
		
		BankMessage receiveMsg;
		long startTime = System.currentTimeMillis();
		while ((receiveMsg = queue.getMessage()) == null) {
			// wait until we received message
			if (isTimeout(startTime)) {
				throw new Exception("Timeout while waiting for result message");
			}
		}
		
		assertTrue("Sended message is not of type BalanceRequestMessage",
				receiveMsg instanceof BalanceRequestMessage);
		assertTrue("Sended message does not equal received msg\n"+sendMsg+"\n"+receiveMsg,
				sendMsg.equals(receiveMsg));
	}
	
	@Test
	public void testMultipleMessages() throws Exception {
		initQueue("testMultipleMessages");
		
		List<BankMessage> sendList = createMessageList();
		
		for (BankMessage m : sendList) {
			queue.sendMessage(m);
		}
		
		List<BankMessage> receiveList = new ArrayList<BankMessage>();
		long startTime = System.currentTimeMillis();
		while (sendList.size() > receiveList.size()) {
			List<BankMessage> l = queue.getMessages();
			receiveList.addAll(l);
			
			// wait until we received message
			if (isTimeout(startTime)) {
				throw new Exception("Timeout while waiting for result message");
			}
		}
		
		Comparator<BankMessage> txComparator = new Comparator<BankMessage>() {
			@Override
			public int compare(BankMessage o1, BankMessage o2) {
				return o1.getTransactionId().compareTo(o2.getTransactionId());
			}
		};
		
		// Sort lists because SQS does not guarantee FIFO
		Collections.sort(sendList, txComparator);
		Collections.sort(receiveList, txComparator);
		
		assertEquals("Sended messages does not equal the received messages", sendList, receiveList);
	}
}
