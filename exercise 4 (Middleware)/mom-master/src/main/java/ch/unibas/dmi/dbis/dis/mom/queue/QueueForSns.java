package ch.unibas.dmi.dbis.dis.mom.queue;

import ch.unibas.dmi.dbis.dis.mom.message.BankMessage;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.Message;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class QueueForSns<T extends BankMessage> extends Queue<T> {
    private ObjectMapper mapper = new ObjectMapper();

    /**
     * Constructs a new {@link Queue}.
     * If the queue does not exist, it will be created.
     *
     * @param sqs       the SQS object
     * @param queueName the name of {@link Queue}.
     */
    public QueueForSns(AmazonSQS sqs, String queueName) {
        super(sqs, queueName);
    }

    @Override
    protected T createBankMessage(Message msg) {
        String body = null;

        try {
            JsonNode actualObj = mapper.readTree(msg.getBody());
            body = actualObj.get("Message").textValue();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // We can suppress this warning because we know that
        // messages have to be of type T because we send only messages of type T
        @SuppressWarnings("unchecked")
        T _return = (T) BankMessage.create(body, msg.getReceiptHandle());
        return _return;
    }
}
