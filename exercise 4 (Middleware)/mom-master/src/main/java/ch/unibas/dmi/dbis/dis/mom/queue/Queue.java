package ch.unibas.dmi.dbis.dis.mom.queue;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import ch.unibas.dmi.dbis.dis.mom.message.BankMessage;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;

/**
 * A class that wraps the functionality of {@link AmazonSQS SQS} queues.
 * 
 * @author Lukas Beck, HS 2012
 *
 * @param <T> type of {@link BankMessage BankMessages} that should be sent using this queue.
 */
public class Queue<T extends BankMessage> implements Closeable {
	private AmazonSQS sqs;
	private String queueName;
	private String url;
	
	/**
	 * Constructs a new {@link Queue}.
	 * If the queue does not exist, it will be created.
	 * 
	 * @param sqs the SQS object
	 * @param queueName the name of {@link Queue}.
	 */
	public Queue(AmazonSQS sqs, String queueName) {
		this.sqs = sqs;
		this.queueName = queueName;
		CreateQueueResult result = this.sqs.createQueue(new CreateQueueRequest(this.queueName));
		this.url = result.getQueueUrl();
	}

	/**
	 * Sends a message to this queue.
	 * 
	 * @param msg message
	 */
	public void sendMessage(T msg) {
		this.sqs.sendMessage(new SendMessageRequest(url, msg.toString()));
	}
	
	/**
	 * Returns the next available message.
	 * Notice that Amazon SQS does not implement a FIFO.
	 * 
	 * @return {@code null} if no messages are available yet or the next message
	 */
	public T getMessage() {
		ReceiveMessageResult result = this.sqs.receiveMessage(new ReceiveMessageRequest(this.url).withMaxNumberOfMessages(1));
		List<Message> msgs = result.getMessages();
		if (msgs.size() < 1) {
			return null;
		}
		
		return createBankMessage(msgs.get(0));
	}
	
	protected T createBankMessage(Message msg) {
		// We can suppress this warning because we know that
		// messages have to be of type T because we send only messages of type T
		@SuppressWarnings("unchecked")
		T _return = (T) BankMessage.create(msg.getBody(), msg.getReceiptHandle());
		return _return;
	}
	
	/**
	 * @return all available messages or an empty list
	 */
	public List<T> getMessages() {
		ReceiveMessageResult result = this.sqs.receiveMessage(new ReceiveMessageRequest(this.url));
		List<T> _return = new ArrayList<T>();
		
		for (Message m : result.getMessages()) {
			_return.add(createBankMessage(m));
		}
		
		return _return;
	}
	
	/**
	 * Deletes the given message from the queue.
	 * 
	 * @param msg message to be deleted
	 */
	public void deleteMessage(T msg) {
		this.sqs.deleteMessage(new DeleteMessageRequest(this.url, msg.getReceiptHandle()));
	}
	
	/**
	 * Deletes a list of messages.
	 * 
	 * @param msgs list of messages
	 */
	public void deleteMessages(List<T> msgs) {
		for (T msg : msgs) {
			this.deleteMessage(msg);
		}
	}
	
	/**
	 * Clears all messages in this queue.
	 * Because there is sometimes a delay in the message transmission,
	 * this method polls the available messages as long as there are new messages and waits 500ms.
	 * Still, this method cannot guarantee that all messages are deleted
	 * as some message may be yet in flight and not available yet.
	 * For a consistent deletion of all messages, use {@link #close()} to delete the queue and all its messages.
	 */
	public void deleteAllMessages() {
		List<Message> list = this.sqs.receiveMessage(new ReceiveMessageRequest(this.url)).getMessages();
		
		while (list.size() > 0) {
			for (Message m : list) {
				this.sqs.deleteMessage(new DeleteMessageRequest(this.url, m.getReceiptHandle()));
			}
			
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {} 
			
			list = this.sqs.receiveMessage(new ReceiveMessageRequest(this.url)).getMessages();
		}
	}

	public String getUrl() {
		return this.url;
	}

	@Override
	/**
	 * Closes and deletes the queue.
	 * All messages held by the queue are deleted too.
	 * 
	 * This method has no effect if called multiple times.
	 * Additionally, it has no effect if the queue has been deleted already but instead displays a warning.
	 */
	public void close() throws IOException {
		if (sqs != null) {
			try {
				sqs.deleteQueue(new DeleteQueueRequest(this.url));
			} catch (AmazonServiceException e) {
				if (e.getErrorCode().equals("AWS.SimpleQueueService.NonExistentQueue")) {
					System.err.println("Warning: trying to delete an non existent queue " + this.queueName + "(" + this.url + ")");
				}
				else {
					throw e;
				}
			}
			
			this.sqs = null;
			this.url = null;
			this.queueName = null;
		}
	}
}
