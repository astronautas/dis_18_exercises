package ch.unibas.dmi.dbis.dis.mom.message;

/**
 * An abstract class representing all requests.
 * Every request message should inherit from this class.
 * 
 * @author Lukas Beck, HS 2012
 */
public abstract class RequestMessage extends BankMessage {
	protected RequestMessage(String txId) {
		super(txId);
	}
}
