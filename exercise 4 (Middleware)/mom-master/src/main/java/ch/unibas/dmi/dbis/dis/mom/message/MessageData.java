package ch.unibas.dmi.dbis.dis.mom.message;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import com.amazonaws.services.sqs.model.Message;

/**
 * This class holds the data to be sent with {@link Message SQS messages}.
 * It provides the functionality to pack and unpack the data from and into a {@link String}. 
 * 
 * @author Lukas Beck, HS 2012
 */
public class MessageData {
	private static final char CLAZZ_SEPARATOR = '%';
	private static final char TX_SEPARATOR = '!';
	private static final char PARAM_EQUALS = '=';
	private static final char PARAM_SEPARATOR = ',';
	
	/**
	 * This represents the underlying class that holds this data.
	 */
	Class<? extends BankMessage> clazz;
	
	/**
	 * The transaction id
	 */
	String txId;
	
	/**
	 * A {@link Map} containing the actual data of the message.
	 */
	Map<String, String> parameters;
	
	/**
	 * Constructs a new {@link MessageData}.
	 * 
	 * @param clazz underlying class that represents this data
	 * @param txId transaction id
	 * @param parameters parameters
	 */
	public MessageData(Class<? extends BankMessage> clazz, String txId, Map<String, String> parameters) {
		this.clazz = clazz;
		this.txId = txId;
		this.parameters = parameters;
	}
	
	/**
	 * Constructs a new {@link MessageData} from a {@link String}.
	 * Essentially, this method unpacks the {@link String} and fills the variables.
	 * 
	 * @param s {@link String} containing the previously packed {@link MessageData}.
	 */
	public MessageData(String s) {
		int clazzIndex = s.indexOf(CLAZZ_SEPARATOR);
		if (clazzIndex < 0) {
			throw new IllegalArgumentException("Invalid string: no class separator '" + CLAZZ_SEPARATOR + "' in " + s);
		}
		
		int txIndex = s.indexOf(TX_SEPARATOR, clazzIndex+1);
		if (txIndex < 0) {
			throw new IllegalArgumentException("Invalid string: no transaction separator '" + TX_SEPARATOR + "' in " + s);
		}
		
		String clazzString = s.substring(0, clazzIndex);
		Class<? extends BankMessage> clazz;
		try {
			clazz = Class.forName(clazzString).asSubclass(BankMessage.class);
		} catch (ClassNotFoundException e) {
			throw new IllegalArgumentException("Invalid string: invalid class name '" + clazzString + "' in " + s);
		}
		
		String txId = s.substring(clazzIndex+1, txIndex);		
		String messageBody = s.substring(txIndex+1);		
		
		this.clazz = clazz;
		this.txId = txId;
		this.parameters = parseParameters(messageBody);
	}
	
	private static Map<String, String> parseParameters(String parameters) {
		HashMap<String, String> map = new HashMap<String, String>();
		for (String s : parameters.split(String.valueOf(PARAM_SEPARATOR))) {
			String[] split = s.split(String.valueOf(PARAM_EQUALS));
			
			if (split.length != 2) {
				System.err.print("Invalid key-value pair ('" + s + "') because ");
				if (split.length < 2) {
					System.err.print("there needs to be a separator");
				}
				else {
					System.err.print("there are too many separators");
				}
				System.err.println(" '" + PARAM_EQUALS + "', skipping it");
				continue;
			}
			
			if (map.containsKey(split[0])) {
				System.err.println("Warning: Overwriting value " + map.get(split[0]) + " of key " + split[0] + ".");
			}
			map.put(split[0], split[1]);
		}			
		return map;
	}
	
	/**
	 * This method packs the {@link MessageData} into the {@link String} format
	 * by using several separators.
	 * Ideally, the generated {@link String} is sent with {@link Message SQS messages}
	 * and unpacked using {@link #MessageData(String)}.
	 */
	@Override
	public String toString() {
		StringBuilder strBuilder = new StringBuilder(this.clazz.getName() + CLAZZ_SEPARATOR + this.txId + TX_SEPARATOR);
		
		Set<Entry<String, String>> entrySet = this.parameters.entrySet();
		Iterator<Entry<String, String>> itr = entrySet.iterator();
		while (itr.hasNext()) {
			Entry<String, String> e = itr.next();
			strBuilder.append(e.getKey());
			strBuilder.append(PARAM_EQUALS);
			strBuilder.append(e.getValue());
			
			if (itr.hasNext()) {
				strBuilder.append(PARAM_SEPARATOR);
			}
		}
		
		return strBuilder.toString();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((clazz == null) ? 0 : clazz.hashCode());
		result = prime * result
				+ ((parameters == null) ? 0 : parameters.hashCode());
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
		if (!(obj instanceof MessageData)) {
			return false;
		}
		MessageData other = (MessageData) obj;
		if (clazz == null) {
			if (other.clazz != null) {
				return false;
			}
		} else if (!clazz.equals(other.clazz)) {
			return false;
		}
		if (parameters == null) {
			if (other.parameters != null) {
				return false;
			}
		} else if (!parameters.equals(other.parameters)) {
			return false;
		}
		if (txId == null) {
			if (other.txId != null) {
				return false;
			}
		} else if (!txId.equals(other.txId)) {
			return false;
		}
		return true;
	}
}
