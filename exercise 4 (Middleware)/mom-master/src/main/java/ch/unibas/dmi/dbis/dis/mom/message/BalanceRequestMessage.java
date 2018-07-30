package ch.unibas.dmi.dbis.dis.mom.message;

import java.util.HashMap;
import java.util.Map;

/**
 * A {@link RequestMessage} to request the balance of a given account.
 * 
 * @author Lukas Beck, HS 2012
 */
public class BalanceRequestMessage extends RequestMessage {
	private String iban;
	
	/**
	 * Constructs a new {@link BalanceRequestMessage}.
	 * 
	 * @param txId transaction id
	 * @param iban account identifier of the requested account
	 */
	public BalanceRequestMessage(String txId, String iban) {
		super(txId);
		this.iban = iban;
	}
	
	public String getIban() {
		return iban;
	}
	
	protected BalanceRequestMessage(MessageData data) {
		super(data.txId);
		checkIfParameterIsSet(data.parameters, "iban");
		
		this.iban = data.parameters.get("iban");
	}

	@Override
	protected Map<String, String> getParameters() {
		HashMap<String, String> parameters = new HashMap<String, String>();
		parameters.put("iban", iban);
		return parameters;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((iban == null) ? 0 : iban.hashCode());
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
		if (!(obj instanceof BalanceRequestMessage)) {
			return false;
		}
		BalanceRequestMessage other = (BalanceRequestMessage) obj;
		if (iban == null) {
			if (other.iban != null) {
				return false;
			}
		} else if (!iban.equals(other.iban)) {
			return false;
		}
		return true;
	}
}
