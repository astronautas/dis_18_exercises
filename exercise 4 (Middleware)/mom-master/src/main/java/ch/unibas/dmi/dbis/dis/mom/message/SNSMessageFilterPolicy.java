package ch.unibas.dmi.dbis.dis.mom.message;

import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.model.SetSubscriptionAttributesRequest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class SNSMessageFilterPolicy {

    private enum AttributeType {
        String, Numeric, Prefix, List, AnythingBut
    }

    private class Attribute<T> {

        protected final T value;
        protected final AttributeType type;

        public  Attribute(AttributeType type, T value) {
            this.value = value;
            this.type = type;
        }

        public String toString() {
            switch(type) {
                case Prefix:
                    return String.format("{\"prefix\":\"%s\"}", value.toString());
                case Numeric:
                    return String.format("{\"numeric\":%s}", value.toString());
                case List:
                    ArrayList<T> values = (ArrayList<T>)value;
                    return values
                            .stream()
                            .map(entry -> entry.toString())
                            .collect(Collectors.joining(",", "[", "]"));
                case AnythingBut:
                    return String.format("{\"anything-but\":\"%s\"}", value);
                default:
                    return String.format("\"%s\"", value);
            }
        }
    }

    private class NumericValue<T extends Number> {
        private final T lower;
        private final T upper;
        private final String lowerOp;
        private final String upperOp;

        public NumericValue(String op, T value)  {
            lower = value;
            lowerOp = op;
            upper = null;
            upperOp = null;
        }

        public NumericValue(String lowerOp, T lower, String upperOp, T upper) {
            this.lower = lower;
            this.lowerOp = lowerOp;
            this.upper = upper;
            this.upperOp = upperOp;
        }

        public String toString() {
            StringBuffer s = new StringBuffer("[")
                    .append('\"').append(lowerOp).append("\",").append(lower);
            if (upper != null) {
                s.append(",\"").append(upperOp).append("\",").append(upper);
            }
            s.append("]");
            return s.toString();
        }
    }

    private final Map<String, Attribute> filterPolicy = new HashMap<>();

    public void addAttribute(String attributeName, String attributeValue) {
        filterPolicy.put(attributeName, new Attribute(AttributeType.String, attributeValue));
    }

    public void addAttribute(String attributeName, ArrayList<String> attributeValues) {
        ArrayList<Attribute> attributes = new ArrayList<>();
        for (String s : attributeValues) {
            attributes.add(new Attribute(AttributeType.String, s));
        }
        filterPolicy.put(attributeName, new Attribute(AttributeType.List, attributes));
    }

    public void addAttributePrefix(String attributeName, String prefix) {
        filterPolicy.put(attributeName, new Attribute(AttributeType.Prefix, prefix));
    }

    public void addAttributeAnythingBut(String attributeName, String value) {
        filterPolicy.put(attributeName, new Attribute(AttributeType.AnythingBut, value));
    }

    public <T extends Number>  void addAttribute(String attributeName, String op, T value) {
        filterPolicy.put(attributeName, new Attribute(AttributeType.Numeric, new NumericValue(op, value)));
    }

    public <T extends Number>  void addAttributeRange(
            String attributeName, String lowerOp, T lower, String upperOp, T upper) {
        filterPolicy.put(
                attributeName,
                new Attribute(AttributeType.Numeric, new NumericValue(lowerOp, lower, upperOp, upper)));
    }

    public void apply(AmazonSNS snsClient, String subscriptionArn) {
        SetSubscriptionAttributesRequest request =
                new SetSubscriptionAttributesRequest(subscriptionArn, "FilterPolicy", formatFilterPolicy());
        snsClient.setSubscriptionAttributes(request);
    }

    public String formatFilterPolicy() {
        return filterPolicy.entrySet()
                .stream()
                .map(entry -> "\"" + entry.getKey() + "\": [" + entry.getValue() + "]")
                .collect(Collectors.joining(", ", "{", "}"));
    }
}