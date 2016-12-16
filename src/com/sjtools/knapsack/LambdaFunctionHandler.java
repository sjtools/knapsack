package com.sjtools.knapsack;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.EC2ContainerCredentialsProviderWrapper;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.amazonaws.services.lambda.runtime.events.SNSEvent.SNSRecord;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;

public class LambdaFunctionHandler implements RequestHandler<SNSEvent, Object> {

	private static final String ARN_TO_AWS_CHALLENGE_OUT_TOPIC = "arn:aws:sns:eu-west-1:615271911145:gdn-aws-coding-fun-out";
	private static final String ARN_TO_MY_OWN_OUT_TOPIC = "arn:aws:sns:eu-west-1:479663651490:sjtools_test_topic_inout";

	private static String DEFAULT_MESSAGE = "\nslawomir.jercha@gmail.com"; //at least this is published
	
	@Override
    public Object handleRequest(SNSEvent input, Context context) {
		KnapsackStatusData knapsackData = new KnapsackStatusData();    	
    	long timestamp = System.currentTimeMillis();

    	if (null==input || input.getRecords()==null || input.getRecords().get(0)==null || input.getRecords().get(0).getSNS()==null)
    	{//invalid input format, respond with default response
    		context.getLogger().log(knapsackData.wrapLogMessage("Missing input. Null object provided in input."));
    		setResponseMessage("", knapsackData);
    		knapsackData.subject = null;
    		publishKnapsack(knapsackData,context);
    	}
    	else
    	{
	    	//get input data
    		knapsackData.subject = input.getRecords().get(0).getSNS().getSubject();
    		knapsackData.message_in = input.getRecords().get(0).getSNS().getMessage();
	    	String topicArn = input.getRecords().get(0).getSNS().getTopicArn();
	    	if (isSNSFromMyOwnTopic(topicArn))
	    	{//echo of previously send result or warmup from cloudwatch, ignore
	    		context.getLogger().log("Message from my own topic. Subject: " + knapsackData.subject);
	    		context.getLogger().log("Message from my own topic. Message: " + knapsackData.message_in);
	    	}
	    	else
	    	{//do the magic here parse and pack
	    		int a = 100;
	    		int b = 200;
		    	long timestamp2 = System.currentTimeMillis();
		    	boolean isValid =parseAndValidateInput(context, knapsackData); 
		    	context.getLogger().log(knapsackData.wrapLogMessage("parseAndValidateInput() time: " + 0.001*(System.currentTimeMillis()-timestamp2) + " sec"));
		    	if (isValid)
		    	{//do the packing
		        	packKnapsack(context,knapsackData);
		    	}    	
		    	else
		    	{
		    		/*
		    		 * do nothing here, we ended up here because:
		    		 * - either there was a problem with input
		    		 * - or we found solution already while parsing
		    		 */
		    	}
		    	publishKnapsack(knapsackData, context);
	    	}
	    }
    	context.getLogger().log(knapsackData.wrapLogMessage("handleRequest() time: " + 0.001*(System.currentTimeMillis()-timestamp) + " sec"));
        return null;
    }

    /*
     *  packs knapsack
     */
   	private void packKnapsack(Context context, KnapsackStatusData data) 
   	{
   		long timestamp = System.currentTimeMillis();
   		int diff1=Integer.MAX_VALUE, diff2=Integer.MAX_VALUE;
   		EagerKnapsackPacker packer; //resulted packer
   		EagerKnapsackPacker packer1 = new EagerKnapsackPacker(data.items,  data.totalItemsWeight ,data.knapsackCapacity);
   		EagerKnapsackPacker packer2 = new EagerKnapsackPacker(data.items,  data.totalItemsWeight ,data.knapsackCapacity);
   		
   		//classic packing
   		packer1.pack(context.getLogger(), true);
   		diff1 = data.knapsackCapacity - packer1.getPackedWeight();
   		if (diff1 != 0)
   		{// try adaptive packing and compare	
   			packer2.pack(context.getLogger(), false);
   			diff2 = data.knapsackCapacity - packer2.getPackedWeight();
   			packer = (diff1>diff2) ? packer2 : packer1; //pick better result
   		}
   		else
   		{
   			packer = packer1;
   		}
   		data.packedWeight = packer.getPackedWeight(); 
   		setResponseMessage(printPackedItemsList(packer.getPackedItems()), data);
   		context.getLogger().log(data.wrapLogMessage("packer_classic_diff: " + diff1 + 
   													" packer_adaptive_diff: " + 
   													((diff2==Integer.MAX_VALUE) ? "not used" : diff2)));
   		context.getLogger().log(data.wrapLogMessage("packKnapsack() time: "  + 0.001*(System.currentTimeMillis()-timestamp) + " sec"));		
   		return;
   	}
		
/*
 * validates message:
 * - if there is any problem with data provided, then sets error message and returns false
 * - if there is no problem with data, and resolution found while parsing , sets response message and returns false
 * - if there is no problem with data, and packing is required to get solutions, then returns true
 */
	private boolean parseAndValidateInput(Context context, KnapsackStatusData data) 
	{
		if (null==data.message_in)
		{//no input data provided, return default response
			context.getLogger().log(data.wrapLogMessage("Message was null."));
			setResponseMessage("", data);
			return false;
		}
		String[] parts = data.message_in.split("\\s");
		if (parts==null || parts.length==0)
		{
			context.getLogger().log(data.wrapLogMessage("Empty message"));
			setResponseMessage("", data);
			return false;
		}
		//fill items, with sorting descending already
		data.items = new LinkedList<Integer>();
		//min value provided
		int min = Integer.MAX_VALUE;
		
		for (int i = 0; i < parts.length ; i++)
		{
			int value = -1;
			String s = parts[i];
			try
			{
				value = Integer.valueOf(s);
				if (value<0)
				{
					context.getLogger().log(data.wrapLogMessage("Input cannot be less than 0: " + value));
					continue;
				}
				if (i==0)
				{
					data.knapsackCapacity=value;
				}
				else
				{//find smaller and insert
					min = (value < min) ? value : min;
					data.totalItemsWeight += value;
					if (data.items.size()==0)
					{//first item
						data.items.add(value);
					}
					else
					{//next items
						int insert_idx = 0;
						boolean inserted = false;
						for (Iterator<Integer> idx = data.items.iterator(); idx.hasNext();)
						{
							if (idx.next().intValue()<=value)
							{//insert at previous index 
								data.items.add(insert_idx, value);
								inserted = true;
								break;
							}
							else
							{
								insert_idx++;
							}
						}
						if (!inserted)
						{//the smallest, insert at the end
							data.items.add(data.items.size(), value);
						}
						//check if we already have solution and no need to parse further
						if (value == data.knapsackCapacity)
						{//single item packs full knapsack, let's use it as a result
							context.getLogger().log(data.wrapLogMessage("Single item found to fill up knapsack"));
							setResponseMessage(value, data);
							data.packedWeight = data.knapsackCapacity;
							return false;											
						}
						if (data.totalItemsWeight == data.knapsackCapacity)
						{//already parsed items fill capacity
							context.getLogger().log(data.wrapLogMessage("So far parsed items fill up knapsack"));
							data.packedWeight = data.knapsackCapacity;
							setResponseMessage(printPackedItemsList(data.items), data);
							return false;																		
						}
					}
				}
			}
			catch (Exception e)
			{
				context.getLogger().log(data.wrapLogMessage("Invalid entry: " + s));
			}
		}
		//capacity is invalid, error
		if (data.knapsackCapacity<0)
		{
			context.getLogger().log(data.wrapLogMessage("Invalid knapsack capacity"));
			setResponseMessage("", data);
			return false;				
		}
		//knapsack has no capacity
		if (data.knapsackCapacity==0)
		{
			if (data.items.size()==0 || data.totalItemsWeight==0)
			{//nothing to put, quick empty list response
				context.getLogger().log(data.wrapLogMessage("Knapsack capacity is 0 and nothing to put there"));
				setResponseMessage("", data);				
				return false;
			}
			if (min!=0)
			{//there were items to put, impossible to add, error
				context.getLogger().log(data.wrapLogMessage("Knapsack capacity is 0 and there are only non-zero items to put. Capacity: " + data.knapsackCapacity + ", min item: " + min));
				setResponseMessage("", data);
				return false;				
			}
			else
			{//there was 0 item to put, quick good response
				context.getLogger().log(data.wrapLogMessage("Knapsack capacity is 0 and there are zero items to put. Capacity: " + data.knapsackCapacity + ", min item: " + min));
				setResponseMessage(0, data);
				return false;				
			}
			
		}
		//Knapsack has capacity
		//no items to put, error
		if (data.items.size()==0)
		{
			context.getLogger().log(data.wrapLogMessage("No items to put to knapsack with capacity:  " + data.knapsackCapacity));
			setResponseMessage("", data);
			return false;				
		}
		
		// none of the item fit into knapsack, error
		if (data.knapsackCapacity<min)
		{
			context.getLogger().log(data.wrapLogMessage("All items are too big for a knapsack "));
			setResponseMessage("", data);
			return false;				
		}
		
		// all parsed items fit into knapsack already, good response
		if (data.knapsackCapacity>=data.totalItemsWeight)
		{
			context.getLogger().log(data.wrapLogMessage("All items fit into knapsack "));
			data.packedWeight = (int)data.totalItemsWeight;
			setResponseMessage(printPackedItemsList(data.items), data);
			return false;				
		}
		
		return true;
	}

	private void publishKnapsack(KnapsackStatusData data, Context context) 
	{
		long timestamp = System.currentTimeMillis();
		context.getLogger().log(data.wrapLogMessage("knapsack capacity [used/total]: " + data.packedWeight + "/" + data.knapsackCapacity));    			
		context.getLogger().log(data.wrapLogMessage("subject out:  "+ data.subject));    			
		context.getLogger().log(data.wrapLogMessage("message out: " + data.message_out));
		context.getLogger().log(data.wrapLogMessage("sorted and parsed items: " + data.items));
		context.getLogger().log(data.wrapLogMessage("Print log time: "  + 0.001*(System.currentTimeMillis()-timestamp) + " sec"));
		
		PublishRequest publishRequest = null;
		AmazonSNSClient snsClient = null;
		PublishResult publishResult = null;
		
		AWSCredentialsProvider awsCredProvider = getAWSCredentialsProvider(data.subject);
								
		//create publish item to Amazon Challenge
		if (isSendToAmazonChallenge(data.subject))
		{
			try
			{
		        publishRequest = new PublishRequest(ARN_TO_AWS_CHALLENGE_OUT_TOPIC, data.message_out);
		        publishRequest.setSubject(data.subject);
		        
				//create sns client, publish item to topic
				snsClient = new AmazonSNSClient(awsCredProvider);
		        snsClient.setRegion(Region.getRegion(Regions.EU_WEST_1));
		        publishResult = snsClient.publish(publishRequest);
		        context.getLogger().log(data.wrapLogMessage("Publish Successful to Amazon Challenge" + publishResult.getMessageId()));
			}
			catch(Exception e)
			{
				context.getLogger().log(data.wrapLogMessage("Publish to Amazon Challenge failed : " + e.getMessage()));
			}
			finally
			{
				try
				{
					if (null!=snsClient)
				        snsClient.shutdown();
				}
				catch (Exception e)
				{
					//do not do anything here
				}
			}
			context.getLogger().log(data.wrapLogMessage("Publish to Amazon Challenge time: "  + 0.001*(System.currentTimeMillis()-timestamp) + " sec"));
			timestamp = System.currentTimeMillis();
		}
        // same to my topic
		try
		{
			
	        publishRequest = new PublishRequest(ARN_TO_MY_OWN_OUT_TOPIC, data.message_out);
	        publishRequest.setSubject(data.subject);
			//create sns client, publish item to topic
			snsClient = new AmazonSNSClient(awsCredProvider);
	        snsClient.setRegion(Region.getRegion(Regions.EU_WEST_1));
	        publishResult = snsClient.publish(publishRequest);
	        context.getLogger().log(data.wrapLogMessage("Publish Successful to my topic" + publishResult.getMessageId()));
	        snsClient.shutdown();
		}
		catch(Exception e)
		{
			context.getLogger().log(data.wrapLogMessage("Publish to my topic failed : " + e.getMessage()));
		}
		finally
		{
			try
			{
				if (null!=snsClient)
			        snsClient.shutdown();
			}
			catch (Exception e)
			{
				//do not do anything here
			}
		}
		context.getLogger().log(data.wrapLogMessage("Publish to my topic time: "  + 0.001*(System.currentTimeMillis()-timestamp) + " sec"));        
	}

/*
 * this is for testing in the cloud - check what provider to use, depending on subject
 */
private AWSCredentialsProvider getAWSCredentialsProvider(String subject) 
	{
		if (null!=subject && subject.contains("sjtools-EnvironmentVariableCredentialsProvider"))
		{
				return new EnvironmentVariableCredentialsProvider();
		}
		if (null!=subject && subject.contains("sjtools-SystemPropertiesCredentialsProvider"))
		{
				return new SystemPropertiesCredentialsProvider();
		}
		if (null!=subject && subject.contains("sjtools-ProfileCredentialsProvider"))
		{
				return new ProfileCredentialsProvider();
		}
		if (null!=subject && subject.contains("sjtools-EC2ContainerCredentialsProviderWrapper"))
		{
				return new EC2ContainerCredentialsProviderWrapper();
		}
		if (null!=subject && subject.contains("sjtools-DefaultAWSCredentialsProviderChain"))
		{
				return new DefaultAWSCredentialsProviderChain();
		}		
		//default for lambda
		return new EnvironmentVariableCredentialsProvider();
	}

/*
 * this is for testing in the cloud - check whether to send to Amazon Challenge or to my topic only
 */

	private boolean isSendToAmazonChallenge(String subject)
	{
		if (null!=subject && subject.contains("sjtools"))
		{
			return false;
		}
		else
		{
			return true;
		}
	}

    /*
     * check if message is echo of message sent out to my own out topic
     */
    private boolean isSNSFromMyOwnTopic(String receivedTopicArn) 
    {
		return ARN_TO_MY_OWN_OUT_TOPIC.equalsIgnoreCase(receivedTopicArn);
	}
	
	private String printPackedItemsList(List<Integer> items) 
	{
		StringBuilder sb = new StringBuilder();
		items.forEach(it -> sb.append(it).append(" "));
		sb.setLength(sb.length()-1);
		return sb.toString();
	}

	private void setResponseMessage(int code, KnapsackStatusData knapsackData) 
	{
		knapsackData.message_out = code + DEFAULT_MESSAGE;
	}

	private void setResponseMessage(String message, KnapsackStatusData knapsackData) 
	{
		knapsackData.message_out = message + DEFAULT_MESSAGE;
	}
	public static void main(String[] args)
	{
		LambdaFunctionHandler handler = new LambdaFunctionHandler();		
		System.out.println("Hellow");
	}
}
