package ch.unibas.dmi.dbis.dis.mom.message;

import java.lang.reflect.Constructor;
import java.util.Map;

/**
 * Message class that individual message types inherit from.
 * Every message has a transaction id.
 * 
 * Classes that inherit from this class <b>must</b> implement a (protected) constructor
 * that takes a {@link MessageData}.
 * 
 * @author Lukas Beck, HS 2012
 */
public abstract class BankMessage {
	private String txId;
	private String receiptHandle;
	
	/**
	 * @return the associated transaction id
	 */
	public final String getTransactionId() {
		return this.txId;
	}
	
	/**
	 * @return the receipt handle of the internal {@link com.amazonaws.services.sqs.model.Message SQS message}
	 */
	public final String getReceiptHandle() {
		return receiptHandle;
	}
	
	protected void setReceiptHandle(String receiptHandle) {
		this.receiptHandle = receiptHandle;
	}
	
	/** 
	 * Returns the string representation of the message containing:
	 * <ul>
	 * <li>the underlying {@link Class}</li>
	 * <li>the transaction id</li>
	 * <li>the operation {@link String}</li>
	 * <li>and the parameters</li>
	 * </ul>
	 * This representation is used inside of the {@link com.amazonaws.services.sqs.model.Message SQS messages}.
	 * 
	 * @return string representation of the message 
	 */
	@Override
	public final String toString() {
		MessageData data = new MessageData(this.getClass(), getTransactionId(), getParameters());
		return data.toString();
	}
	
	/**
	 * Parses and creates a new BankMessage out of a {@link String} and a given receiptHandle.
	 * 
	 * @param s {@link String} containing the message
	 * @param receiptHandle the receipt handle of the {@link com.amazonaws.services.sqs.model.Message SQS messsage}
	 * @return BankMessage of the string
	 */
	public static BankMessage create(String s, String receiptHandle) {
		// Parse string into MessageData
		MessageData data = new MessageData(s);
		
		try {
			// Get constructor of given class that takes MessageData
			Constructor<? extends BankMessage> constructor = data.clazz.getDeclaredConstructor(MessageData.class);
			
			// Create new BankMessage
			BankMessage msg = constructor.newInstance(data);			
			msg.setReceiptHandle(receiptHandle);
			return msg;
		} catch (Exception e) {
			System.err.println(e);
			throw new IllegalArgumentException(e.getClass().getName() + ": " + e.getMessage());
		}
	}
	
	protected BankMessage(String txId) {
		this.txId = txId;
	}
	
	protected abstract Map<String, String> getParameters();
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((receiptHandle == null) ? 0 : receiptHandle.hashCode());
		result = prime * result + ((txId == null) ? 0 : txId.hashCode());
		return result;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof BankMessage)) {
			return false;
		}
		BankMessage other = (BankMessage) obj;
		if (txId == null) {
			if (other.txId != null) {
				return false;
			}
		} else if (!txId.equals(other.txId)) {
			return false;
		}
		return true;
	}
	
	/**
	 * A convenience method for classes that inherit from this class
	 * to check if a key is set in a {@link Map}.
	 * 
	 * @param map map
	 * @param key key
	 * @throws IllegalArgumentException If the {@code key} is not defined in the {@code map}.
	 */
	protected static void checkIfParameterIsSet(Map<String, String> map, String key) {
		if (!map.containsKey(key)) {
			throw new IllegalArgumentException("No parameter key '" + key + "' defined");
		}
	}
}
