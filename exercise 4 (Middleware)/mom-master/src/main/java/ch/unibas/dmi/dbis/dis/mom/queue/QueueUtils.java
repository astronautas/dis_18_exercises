/*
 * Created on Nov 28, 2012
 *
 * (c) 2012 University of Basel Switzerland - Filip-M. Brinkmann
 */

package ch.unibas.dmi.dbis.dis.mom.queue;

import java.io.IOException;
import java.util.List;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.ListQueuesResult;

/**
 * Collection of handy utilities for working with SQS.
 * 
 * @author Filip-M. Brinkmann - filip.brinkmann@unibas.ch
 */
public class QueueUtils {
	
	public static void deleteAllQueues(){
		AmazonSQSClient sqs;
		try {
			sqs = new AmazonSQSClient(new PropertiesCredentials(QueueUtils.class.getResourceAsStream("AwsCredentials.properties")));
			sqs.setEndpoint("sqs.eu-west-1.amazonaws.com");
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}
		ListQueuesResult result = sqs.listQueues();
		List<String> queueUrls = result.getQueueUrls();
		System.out.println("Deleting "+queueUrls.size()+ " queues.");
		for (String url : queueUrls) {
			sqs.deleteQueue(new DeleteQueueRequest(url));
		}
	}
	
	public static void main(String[] args) {
		QueueUtils.deleteAllQueues();
	}

}
