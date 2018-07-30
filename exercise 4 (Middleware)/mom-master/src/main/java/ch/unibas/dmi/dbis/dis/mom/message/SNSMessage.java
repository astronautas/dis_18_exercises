package ch.unibas.dmi.dbis.dis.mom.message;

import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.model.MessageAttributeValue;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class SNSMessage {

    public String message;
    public Map<String, MessageAttributeValue> messageAttributes;

    public SNSMessage(String message) {
        this.message = message;
        messageAttributes = new HashMap<>();
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getMessage() {
        return message;
    }

    public void addAttribute(String attributeName, String attributeValue) {
        MessageAttributeValue messageAttributeValue = new MessageAttributeValue()
                .withDataType("String")
                .withStringValue(attributeValue);
        messageAttributes.put(attributeName, messageAttributeValue);
    }

    public void addAttribute(String attributeName, ArrayList<?> attributeValues) {
        String valuesString, delimiter = ", ", prefix = "[", suffix = "]";
        if (attributeValues.get(0).getClass() == String.class) {
            delimiter = "\", \"";
            prefix = "[\"";
            suffix = "\"]";
        }
        valuesString = attributeValues
                .stream()
                .map(Object::toString)
                .collect(Collectors.joining(delimiter, prefix, suffix));
        MessageAttributeValue messageAttributeValue = new MessageAttributeValue()
                .withDataType("String.Array")
                .withStringValue(valuesString);
        messageAttributes.put(attributeName, messageAttributeValue);
    }

    public void addAttribute(String attributeName, Number attributeValue) {
        MessageAttributeValue messageAttributeValue = new MessageAttributeValue()
                .withDataType("Number")
                .withStringValue(attributeValue.toString());
        messageAttributes.put(attributeName, messageAttributeValue);
    }

    public String publish(AmazonSNS snsClient, String topicArn) {
        PublishRequest request = new PublishRequest(topicArn, message)
                .withMessageAttributes(messageAttributes);
        PublishResult result = snsClient.publish(request);

        return result.getMessageId();
    }
}
