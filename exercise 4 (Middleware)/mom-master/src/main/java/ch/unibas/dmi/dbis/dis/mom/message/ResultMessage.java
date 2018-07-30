package ch.unibas.dmi.dbis.dis.mom.message;

/**
 * An abstract class representing all results.
 * Every result message should inherit from this class.
 * 
 * @author Lukas Beck, HS 2012
 */
public abstract class ResultMessage extends BankMessage {
	protected ResultMessage(String txId) {
		super(txId);
	}
}
