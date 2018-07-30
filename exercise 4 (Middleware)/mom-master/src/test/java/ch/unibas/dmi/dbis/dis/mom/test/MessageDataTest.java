package ch.unibas.dmi.dbis.dis.mom.test;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import ch.unibas.dmi.dbis.dis.mom.message.BalanceResultMessage;
import ch.unibas.dmi.dbis.dis.mom.message.DepositRequestMessage;
import ch.unibas.dmi.dbis.dis.mom.message.MessageData;

/**
 * This class tests the String packing and unpacking of the class {@link MessageData}.
 * 
 * @author Lukas Beck, HS 2012
 */
public class MessageDataTest {
	@Test
	public void testDepositRequest() {
		Map<String, String> parameters = new HashMap<String, String>();
		parameters.put("key", "value");
		MessageData original = new MessageData(DepositRequestMessage.class, "transaction id", parameters);
		String s = original.toString();
		MessageData parsed = new MessageData(s);
		
		assertEquals("Parsed message data is not the same as original", original, parsed);
	}

	@Test
	public void testBalanceResult() {
		Map<String, String> parameters = new HashMap<String, String>();
		parameters.put("i'm a key - yay!", "and i'm a value - hohoho.");
		MessageData original = new MessageData(BalanceResultMessage.class, "another transaction id with $$$", parameters);
		String s = original.toString();
		MessageData parsed = new MessageData(s);
		
		assertEquals("Parsed message data is not the same as original", original, parsed);
	}
}
