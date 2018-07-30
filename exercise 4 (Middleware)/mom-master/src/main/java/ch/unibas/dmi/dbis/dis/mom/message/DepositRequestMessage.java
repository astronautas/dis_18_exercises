package ch.unibas.dmi.dbis.dis.mom.message;

import java.util.HashMap;
import java.util.Map;

/**
 * A {@link RequestMessage} to request deposit of an account.
 * 
 * @author Lukas Beck, HS 2012
 */
public class DepositRequestMessage extends RequestMessage {
	private String bic;
	private String iban;
	private double amount;
	
	/**
	 * Constructs a new {@link DepositRequestMessage}.
	 * 
	 * @param txId transaction id
	 * @param bic target bank identifier which {@code iban} is located on
	 * @param iban target account identifier to deposit the {@code amount} to
	 * @param amount amount to deposit
	 */
	public DepositRequestMessage(String txId, String bic, String iban, double amount) {
		super(txId);
		this.bic = bic;
		this.iban = iban;
		this.amount = amount;
	}
	
	public String getBic() {
		return bic;
	}
	
	public String getIban() {
		return iban;
	}
	
	public double getAmount() {
		return amount;
	}
	
	protected DepositRequestMessage(MessageData data) {
		super(data.txId);		
		checkIfParameterIsSet(data.parameters, "bic");
		checkIfParameterIsSet(data.parameters, "iban");
		checkIfParameterIsSet(data.parameters, "amount");
		
		this.bic = data.parameters.get("bic");
		this.iban = data.parameters.get("iban");
		
		String s = data.parameters.get("amount");
		try {
			this.amount = Double.parseDouble(s); 
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("Value " + s + " of key 'amount' is no valid number");
		}
	}

	@Override
	protected Map<String, String> getParameters() {
		Map<String, String> parameters = new HashMap<String, String>();
		parameters.put("bic", bic);
		parameters.put("iban", iban);
		parameters.put("amount", String.valueOf(amount));
		return parameters;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		long temp;
		temp = Double.doubleToLongBits(amount);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		result = prime * result + ((bic == null) ? 0 : bic.hashCode());
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
		if (!(obj instanceof DepositRequestMessage)) {
			return false;
		}
		DepositRequestMessage other = (DepositRequestMessage) obj;
		if (Double.doubleToLongBits(amount) != Double
				.doubleToLongBits(other.amount)) {
			return false;
		}
		if (bic == null) {
			if (other.bic != null) {
				return false;
			}
		} else if (!bic.equals(other.bic)) {
			return false;
		}
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
