package ch.unibas.dmi.dbis.dis.mom.server;

import ch.unibas.dmi.dbis.dis.mom.exception.*;
import ch.unibas.dmi.dbis.dis.mom.message.*;
import ch.unibas.dmi.dbis.dis.mom.queue.Queue;
import ch.unibas.dmi.dbis.dis.mom.queue.QueueForSns;
import ch.unibas.dmi.dbis.dis.mom.test.TestUtilities;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.sns.model.CreateTopicRequest;
import com.amazonaws.services.sns.model.CreateTopicResult;
import com.amazonaws.services.sns.model.DeleteTopicRequest;
import com.amazonaws.services.sns.util.Topics;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

// SNS coupled with SQS server
// The only difference from BankServer is that SNS is used to deliver messages
// to queues, removing direct server (sender) to queue (recipient) coupling
// + allowing to send to multiple subscribers at once (better future scalability)
public class SnsBankServer extends BankServer {
    private static final String REQUEST_ATTR = "request";
    private static final String RESPONSE_ATTR = "response";
    private static final String REQ_RESP_ATTR_NAME = "requestresponseattr";
    Queue<ResultMessage> myResponseQueue;
    private boolean deleteTopicsAfterwards;
    private AmazonSNS sns;
    private CreateTopicResult myTopic;
    private CreateTopicResult remoteTopic;

    public SnsBankServer(String bic, String remoteBic) throws FileNotFoundException, IOException {
        this(bic, remoteBic, true);
    }

    public SnsBankServer(String bic, String remoteBic, boolean clearTopics) throws FileNotFoundException, IOException {
        this(bic, remoteBic, clearTopics, true);
    }

    public SnsBankServer(String bic, String remoteBic, boolean clearTopics, boolean deleteTopicsAfterwards) throws FileNotFoundException, IOException {
        super(bic, remoteBic, clearTopics, deleteTopicsAfterwards);

        this.bic = bic;
        this.remoteBic = remoteBic;
        this.deleteTopicsAfterwards = deleteTopicsAfterwards;

        this.sns = createSNS(this.awsCredentials);

        this.initializeTopics(clearTopics);
        this.initializeSubscriptions();
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            showHelp();
        }

        boolean insertTestData = false;
        if (args[0].equals("-i")) {
            insertTestData = true;
            args = shiftLeft(args, 1);
        }

        if (args.length < 2) {
            showHelp();
        }

        String bic = args[0];
        String remoteBic = args[1];

        SnsBankServer server = null;
        try {
            server = new SnsBankServer(bic, remoteBic);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        if (insertTestData) {
            TestUtilities.insertTestData(server, true);
        }

        server.start();
    }

    @Override
    public void run() {
        System.out.println("Starting bank server with BIC '" + this.bic + "'.");

        while (true) {
            // Check and handle new request
            RequestMessage requestMessage = myRequestQueue.getMessage();

            if (requestMessage != null) {
                handleRequest(requestMessage);
                myRequestQueue.deleteMessage(requestMessage);
            }

            // Check and handle all results
            List<ResultMessage> resultMessages = myResponseQueue.getMessages();
            handleResults(resultMessages);
            myResponseQueue.deleteMessages(resultMessages);

            // Check for expired transactions
            checkAndCompensateExpiredTransactions();

            try {
                Thread.sleep(250);
            } catch (InterruptedException e) {
                // Stop if it has been interrupted
                break;
            }
        }

        System.out.print("Stopping bank server");

        if (this.deleteQueuesAfterwards) {
            System.out.print(" and deleting our queues...");
            try {
                myRequestQueue.close();
            } catch (IOException e) {
            }
            try {
                myResponseQueue.close();
            } catch (IOException e) {
            }
        } else {
            System.out.print("...");
        }
        System.out.println(" Done");

        if (deleteTopicsAfterwards) {
            DeleteTopicRequest deleteTopicRequest = new DeleteTopicRequest(myTopic.getTopicArn());
            sns.deleteTopic(deleteTopicRequest);
        }
    }

    private AmazonSNS createSNS(AWSCredentials awsCredentials) {
        System.out.print("Instantiating SNS client...");
        AWSStaticCredentialsProvider awsStaticCredentialsProvider = new AWSStaticCredentialsProvider(awsCredentials);

        AmazonSNSClientBuilder amazonSNSClientBuilder = AmazonSNSClientBuilder.standard().withCredentials(awsStaticCredentialsProvider);
        amazonSNSClientBuilder.setRegion("eu-west-3");

        System.out.println(" Done!");

        return amazonSNSClientBuilder.build();
    }

    private void initializeSubscriptions() {
        String subReqArn = Topics.subscribeQueue(sns, sqs, myTopic.getTopicArn(), myRequestQueue.getUrl());
        String subRespArn = Topics.subscribeQueue(sns, sqs, myTopic.getTopicArn(), myResponseQueue.getUrl());

        SNSMessageFilterPolicy fp = new SNSMessageFilterPolicy();
        fp.addAttribute(REQ_RESP_ATTR_NAME, REQUEST_ATTR);
        fp.apply(sns, subReqArn);

        SNSMessageFilterPolicy fp2 = new SNSMessageFilterPolicy();
        fp2.addAttribute(REQ_RESP_ATTR_NAME, RESPONSE_ATTR);
        fp2.apply(sns, subRespArn);
    }

    private void initializeTopics(boolean clearTopics) {
        System.out.print("Initializing topics...");

        CreateTopicRequest myTopicRequest = new CreateTopicRequest(bic);
        myTopic = sns.createTopic(myTopicRequest);

        CreateTopicRequest remoteTopicRequest = new CreateTopicRequest(remoteBic);
        remoteTopic = sns.createTopic(remoteTopicRequest);

        System.out.println(" Done!");

        if (clearTopics) {
            // TODO: I guess no need to clean up, it's no a queue
        }
    }

    @Override
    void initializeQueues(boolean clearQueues) {
        System.out.print("Initializing queues...");
        myRequestQueue = new QueueForSns<RequestMessage>(sqs, REQUEST_PREFIX + bic);
        myResponseQueue = new QueueForSns<ResultMessage>(sqs, RESULT_PREFIX + bic);
        remoteRequestQueue = new QueueForSns<RequestMessage>(sqs, REQUEST_PREFIX + remoteBic);
        remoteResponseQueue = new QueueForSns<DepositResultMessage>(sqs, RESULT_PREFIX + remoteBic);
        System.out.println(" Done!");

        if (clearQueues) {
            System.out.print("Deleting all remaining messages...");
            myRequestQueue.deleteAllMessages();
            myResponseQueue.deleteAllMessages();
            System.out.println(" Done!");
        }
    }

    // HANDLING
    protected void handleResults(List<ResultMessage> resultMessages) {
        for (ResultMessage msg : resultMessages) {
            if (msg instanceof DepositResultMessage) {
                DepositResultMessage msg1 = (DepositResultMessage) msg;

                String txId = msg.getTransactionId();

                if (!transactionTable.containsId(txId)) {
                    System.err.println("Warning: Received result of unknown transaction: " + txId
                            + ", contents of table: " + transactionTable);
                    continue;
                }

                // If deposit failed, compensate it
                if (!msg1.hasSucceded()) {
                    try {
                        compensate(transactionTable.get(txId));
                    } catch (UnknownTransactionException e) {
                        System.err.println("Warning: could not find account while trying to compensate a failed transfer...");
                    }
                }

                try {
                    transactionTable.remove(txId);
                } catch (UnknownTransactionException e) {
                    System.err.println("Warning: could not remove transaction id '" + txId + "' from table");
                }
            } else {
                BalanceResultMessage msg1 = (BalanceResultMessage) msg;

                try {
                    Transaction transaction = transactionTable.get(msg1.getTransactionId());

                    try {
                        transaction.amount = ((BalanceResultMessage) msg).getBalance();
                    } catch (UnknownAccountException e) {
                        e.printStackTrace();
                    }

                } catch (UnknownTransactionException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    public void handleDepositRequest(DepositRequestMessage depositRequest) {
        boolean success = false;

        // Check if we are the correct bank
        if (this.bic.equals(depositRequest.getBic())) {
            try {
                this.localDeposit(depositRequest.getIban(), depositRequest.getAmount());
                success = true; // success is true iff we are the target bank and deposit was successful
            } catch (Exception e) {
                // deposit failed
            }
        } else {
            // Wrong bank
        }

        // Sends the result back
        DepositResultMessage result = new DepositResultMessage(depositRequest.getTransactionId(), success);

        SNSMessage msg = new SNSMessage(result.toString());
        msg.addAttribute(REQ_RESP_ATTR_NAME, RESPONSE_ATTR);

        msg.publish(sns, remoteTopic.getTopicArn());
    }

    @Override
    public void handleBalanceRequest(BalanceRequestMessage balanceRequest) {
        String txId = balanceRequest.getTransactionId();
        String iban = balanceRequest.getIban();

        BalanceResultMessage result;
        try {
            double balance = this.getLocalBalance(iban);
            result = new BalanceResultMessage(txId, balance);
        } catch (UnknownAccountException e) {
            result = new BalanceResultMessage(txId, e);
        }

        SNSMessage msg = new SNSMessage(result.toString());
        msg.addAttribute(REQ_RESP_ATTR_NAME, RESPONSE_ATTR);
        msg.publish(sns, remoteTopic.getTopicArn());
    }

    @Override
    public void deposit(String trxId, String bic, String iban, double amount) throws UnknownAccountException, UnknownBicException, IllegalOperationException {
        if (amount <= 0) {
            throw new IllegalOperationException("The amount to deposit must be positive");
        }

        if (!remoteBic.equals(bic) && !this.bic.equals(bic)) {
            throw new UnknownBicException(bic);
        }

        // Local transfer, non existing iban
        if (this.bic.equals(bic) && !this.database.listAccounts().contains(iban)) {
            throw new UnknownAccountException(bic, iban);
        }

        // Either local deposit or remote
        if (this.bic.equals(bic)) {
            localDeposit(iban, amount);
        } else {
            SNSMessage msg = new SNSMessage(new DepositRequestMessage(trxId, bic, iban, amount).toString());
            msg.addAttribute(REQ_RESP_ATTR_NAME, REQUEST_ATTR);
            msg.publish(sns, remoteTopic.getTopicArn());
        }
    }

    @Override
    public double getBalance(String bic, String iban) throws UnknownAccountException, UnknownBicException, TransactionExpiredException, InterruptedException, UnknownTransactionException {
        if (!remoteBic.equals(bic) && !this.bic.equals(bic)) {
            throw new UnknownBicException(bic);
        }

        if (this.bic.equals(bic)) {
            return getLocalBalance(iban);
        } else {
            String trId = transactionTable.put(new Transaction(iban));

            SNSMessage msg = new SNSMessage(new BalanceRequestMessage(trId, iban).toString());
            msg.addAttribute(REQ_RESP_ATTR_NAME, REQUEST_ATTR);
            msg.publish(sns, remoteTopic.getTopicArn());

            int trExpirationTries = GET_BALANCE_EXPIRATION_TRIES;
            while (trExpirationTries != 0) {

                if (transactionTable.containsId(trId) && transactionTable.get(trId).amount != 0.0) {
                    Transaction transaction = transactionTable.get(trId);
                    return transaction.amount;
                } else {
                    trExpirationTries -= 1;
                }

                Thread.sleep(GET_BALANCE_EXPIRATION_TRY_INTERVAL);
            }

            throw new TransactionExpiredException(trId, transactionTable.get(trId));
        }
    }
}
