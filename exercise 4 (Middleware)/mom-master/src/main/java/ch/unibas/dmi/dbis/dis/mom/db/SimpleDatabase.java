package ch.unibas.dmi.dbis.dis.mom.db;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import ch.unibas.dmi.dbis.dis.mom.exception.AccountOverdrawException;
import ch.unibas.dmi.dbis.dis.mom.exception.KnownAccountException;
import ch.unibas.dmi.dbis.dis.mom.exception.UnknownAccountException;
import ch.unibas.dmi.dbis.dis.mom.server.BankServer;

/**
 * Mockup Database for usage with {@link BankServer}.
 * 
 * @author Filip-M. Brinkmann
 * University of Basel, AS 2011
 * @author Lukas Beck, HS 2012
 */
public class SimpleDatabase implements IDatabase {
	private Map<String, Double> accounts;
	private String bic;
	
	public SimpleDatabase(String bic) {
		this.accounts = new HashMap<String, Double>();
		this.bic = bic;
	}
	
	@Override
	public double getBalance(String iban) throws UnknownAccountException{
		Double result = this.accounts.get(iban);
		if(result == null) {
			throw new UnknownAccountException(this.bic, iban);
		}
		else {
			return this.accounts.get(iban);
		}
	}
	
	@Override
	public void deposit(String iban, double amount) throws UnknownAccountException{
		if(this.accounts.containsKey(iban)){
			double oldBalance = this.accounts.get(iban);
			this.accounts.put(iban, oldBalance+amount);
		}
		else {
			throw new UnknownAccountException(this.bic, iban);
		}
	}
	
	@Override
	public void withdraw(String iban, double amount) throws UnknownAccountException, AccountOverdrawException{
		if(this.accounts.containsKey(iban)){
			double oldBalance = this.accounts.get(iban);
			
			if (oldBalance < amount) {
				throw new AccountOverdrawException(iban);
			}
			
			this.accounts.put(iban, oldBalance-amount);
		}
		else {
			throw new UnknownAccountException(this.bic, iban);
		}
	}

	@Override
	public void addAccount(String iban, double balance)
			throws KnownAccountException {
		if (this.accounts.containsKey(iban)) {
			throw new KnownAccountException(iban);
		}
		
		this.accounts.put(iban, balance);
	}

	@Override
	public void deleteAccount(String iban)
			throws UnknownAccountException {
		if (!this.accounts.containsKey(iban)) {
			throw new UnknownAccountException(this.bic, iban);
		}
		
		this.accounts.remove(iban);
	}

	@Override
	public Set<String> listAccounts() {
		return new HashSet<String>(this.accounts.keySet());
	}

}
