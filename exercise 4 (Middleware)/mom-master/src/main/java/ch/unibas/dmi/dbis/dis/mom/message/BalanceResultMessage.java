package ch.unibas.dmi.dbis.dis.mom.message;

import java.util.HashMap;
import java.util.Map;

import ch.unibas.dmi.dbis.dis.mom.exception.UnknownAccountException;

/**
 * A {@link ResultMessage} that holds the result of a previous {@link BalanceRequestMessage}.
 * 
 * @author Lukas Beck, HS 2012
 */
public class BalanceResultMessage extends ResultMessage {
	private Double balance;
	private UnknownAccountException exception;
	
	/**
	 * Constructs a new BalanceResultMessage with a given balance.
	 * 
	 * @param txId transaction id
	 * @param balance balance of the account
	 */
	public BalanceResultMessage(String txId, double balance) {
		super(txId);
		if (balance < 0) {
			throw new IllegalArgumentException("Balance must be positive");
		}
		
		this.balance = balance;
		this.exception = null;
	}
	
	/**
	 * Constructs a new BalanceResultMessage with an {@link UnknownAccountException}
	 * if the account was unknown.
	 * 
	 * @param txId transaction id
	 * @param e unknown account exception
	 */
	public BalanceResultMessage(String txId, UnknownAccountException e) {
		super(txId);
		if (e == null) {
			throw new NullPointerException("UnknownAccountException mustn't be null");
		}
		
		this.balance = null;
		this.exception = e;
	}
	
	/**
	 * Gets the balance of the result message.
	 * 
	 * @return balance of the requested account
	 * @throws UnknownAccountException if the account was unknown
	 */
	public double getBalance() throws UnknownAccountException {
		if (this.exception != null) {
			throw this.exception;
		}
		
		return balance;
	}
	
	protected BalanceResultMessage(MessageData data) {
		super(data.txId);
		Map<String, String> params = data.parameters;
		
		if (params.containsKey("balance")) {
			String s = params.get("balance");
			try {
				this.balance = Double.parseDouble(s);
			} catch (NumberFormatException e) {
				throw new IllegalArgumentException("Value " + s + " of key 'balance' is no valid number");
			}
			this.exception = null;
		}
		else {
			if (!params.containsKey("exception")) {
				throw new IllegalArgumentException("Either key 'balance' or 'exception' must be defined");
			}
			
			String s = params.get("exception");
			this.exception = UnknownAccountException.parseString(s);
			this.balance = null;
		}
	}

	@Override
	protected Map<String, String> getParameters() {
		HashMap<String, String> parameters = new HashMap<String, String>();
		
		if (this.balance != null) {
			parameters.put("balance", balance.toString());
		}
		else {
			if (this.exception == null) {
				throw new IllegalStateException("Either 'balance' or 'exception' must be not null");
			}
			parameters.put("exception", this.exception.createString());
		}
		
		return parameters;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((balance == null) ? 0 : balance.hashCode());
		result = prime * result
				+ ((exception == null) ? 0 : exception.hashCode());
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
		if (!(obj instanceof BalanceResultMessage)) {
			return false;
		}
		BalanceResultMessage other = (BalanceResultMessage) obj;
		if (balance == null) {
			if (other.balance != null) {
				return false;
			}
		} else if (!balance.equals(other.balance)) {
			return false;
		}
		if (exception == null) {
			if (other.exception != null) {
				return false;
			}
		} else if (!exception.equals(other.exception)) {
			return false;
		}
		return true;
	}

}
