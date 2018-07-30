package ch.unibas.dmi.dbis.dis.mom.test;

import static org.junit.Assert.*;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import ch.unibas.dmi.dbis.dis.mom.exception.UnknownAccountException;
import ch.unibas.dmi.dbis.dis.mom.message.BalanceRequestMessage;
import ch.unibas.dmi.dbis.dis.mom.message.BalanceResultMessage;
import ch.unibas.dmi.dbis.dis.mom.message.BankMessage;
import ch.unibas.dmi.dbis.dis.mom.message.DepositRequestMessage;
import ch.unibas.dmi.dbis.dis.mom.message.DepositResultMessage;

/**
 * This class tests the parsing functions of the {@link BankMessage} implementations.
 * 
 * @author Lukas Beck, HS2012
 */
public class BankMessageTest {	
	private static final String TX_ID = "transactionId";
	private static final String IBAN = "iban";
	private static final String BIC = "bic";
	private static final double DOUBLE = 1337;
	
	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private BankMessage parse(BankMessage original) {
		return BankMessage.create(original.toString(), null);
	}
	
	@Test
	public void testBalanceRequestMessage() {
		BankMessage parsedBankMessage = parse(new BalanceRequestMessage(TX_ID, IBAN));
		
		assertTrue("Parsed message is the wrong class", parsedBankMessage instanceof BalanceRequestMessage);		
		BalanceRequestMessage parsed = (BalanceRequestMessage) parsedBankMessage;
		assertTransactionId(parsed);
		assertEquals("iban is wrong", IBAN, parsed.getIban());
	}

	private void assertTransactionId(BankMessage actual) {
		assertEquals("transaction id is wrong", TX_ID, actual.getTransactionId());
	}

	@Test
	public void testBalanceResultMessage() throws UnknownAccountException {
		BankMessage parsedBankMessage = parse(new BalanceResultMessage(TX_ID, DOUBLE));
		
		assertTrue("Parsed message is the wrong class", parsedBankMessage instanceof BalanceResultMessage);		
		BalanceResultMessage parsed = (BalanceResultMessage) parsedBankMessage;
		assertTransactionId(parsed);
		TestUtilities.assertEquals("balance is wrong", DOUBLE, parsed.getBalance());
	}
	
	@Test
	public void testBalanceResultMessageException() throws UnknownAccountException {
		UnknownAccountException e = new UnknownAccountException(BIC, IBAN);
		BankMessage parsedBankMessage = parse(new BalanceResultMessage(TX_ID, e));
		
		assertTrue("Parsed message is the wrong class", parsedBankMessage instanceof BalanceResultMessage);		
		BalanceResultMessage parsed = (BalanceResultMessage) parsedBankMessage;
		assertTransactionId(parsed);
		TestUtilities.expectUnknownAccountException(thrown, IBAN, BIC);
		parsed.getBalance();
	}
	
	@Test
	public void depositRequestMessage() {
		BankMessage parsedBankMessage = parse(new DepositRequestMessage(TX_ID, BIC, IBAN, DOUBLE));
		
		assertTrue("Parsed message is wrong class", parsedBankMessage instanceof DepositRequestMessage);
		DepositRequestMessage parsed = (DepositRequestMessage) parsedBankMessage;
		assertTransactionId(parsed);
		assertEquals("bic is wrong", BIC, parsed.getBic());
		assertEquals("iban is wrong", IBAN, parsed.getIban());
		TestUtilities.assertEquals("amount is wrong", DOUBLE, parsed.getAmount());
	}
	
	@Test
	public void depositResultMessage() {
		boolean[] array = {true, false};
		for (boolean success : array) {
			BankMessage parsedBankMessage = parse(new DepositResultMessage(TX_ID, success));
			
			assertTrue("Parsed message is wrong class", parsedBankMessage instanceof DepositResultMessage);
			DepositResultMessage parsed = (DepositResultMessage) parsedBankMessage;
			assertTransactionId(parsed);
			assertEquals("Success variable is wrong", success, parsed.hasSucceded());
		}
	}
}
