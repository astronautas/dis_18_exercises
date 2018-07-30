package ch.unibas.dmi.dbis.dis.mom.server;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;


/**
 * Encapsulates a transaction.
 * 
 * @author Ilir Fetai, Filip-M. Brinkmann
 *  University of Basel, HS 2011
 * @author Lukas Beck, HS 2012
 */
public class Transaction implements Serializable {
	private static final long serialVersionUID = -1601116928463463933L;
	private static final SimpleDateFormat DATEFORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	public double amount;
	public String iban;
	public long startTime;
	
	public Transaction(String iban, double amount) {
		this.iban = iban;
		this.amount = amount;
		this.startTime = System.currentTimeMillis();
	}

	public Transaction(String iban) {
		this(iban, 0);
	}
	
	@Override
	public String toString() {
		return "[" + iban + ": " + amount + " - " + DATEFORMAT.format(new Date(startTime)) + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		long temp;
		temp = Double.doubleToLongBits(amount);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		result = prime * result + ((iban == null) ? 0 : iban.hashCode());
		result = prime * result + (int) (startTime ^ (startTime >>> 32));
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
		if (!(obj instanceof Transaction)) {
			return false;
		}
		Transaction other = (Transaction) obj;
		if (Double.doubleToLongBits(amount) != Double
				.doubleToLongBits(other.amount)) {
			return false;
		}
		if (iban == null) {
			if (other.iban != null) {
				return false;
			}
		} else if (!iban.equals(other.iban)) {
			return false;
		}
		if (startTime != other.startTime) {
			return false;
		}
		return true;
	}
}
