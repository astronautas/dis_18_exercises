package ch.unibas.dmi.dbis.dis.mom.message;

import java.util.HashMap;
import java.util.Map;

/**
 * A {@link ResultMessage} that holds the result of a previous {@link DepositRequestMessage}.
 * 
 * @author Lukas Beck, HS 2012
 */
public class DepositResultMessage extends ResultMessage {
	private boolean success;
	
	/**
	 * Constructs a new {@link DepositResultMessage}.
	 * 
	 * @param txId transaction id
	 * @param success boolean to indicate, if the deposit was successful
	 */
	public DepositResultMessage(String txId, boolean success) {
		super(txId);
		this.success = success;
	}
	
	/**
	 * @return true, if the deposit was successful.
	 */
	public boolean hasSucceded() {
		return success;
	}
	
	protected DepositResultMessage(MessageData data) {
		super(data.txId);
		checkIfParameterIsSet(data.parameters, "status");
		
		String s = data.parameters.get("status");
		try {
			this.success = Boolean.parseBoolean(s);
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("Value " + s + " of key 'status' is no valid boolean");
		}
	}

	@Override
	protected Map<String, String> getParameters() {
		HashMap<String, String> parameters = new HashMap<String, String>();
		parameters.put("status", String.valueOf(this.success));
		return parameters;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + (success ? 1231 : 1237);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (!super.equals(obj)) {
			return false;
		}
		if (!(obj instanceof DepositResultMessage)) {
			return false;
		}
		DepositResultMessage other = (DepositResultMessage) obj;
		if (success != other.success) {
			return false;
		}
		return true;
	}
}
