package com.sjtools.knapsack;

import java.util.List;

public class KnapsackStatusData {
	public String subject;
	public String message_in;
	public String message_out;
	public int knapsackCapacity;
	public int packedWeight;
	public List<Integer> items;
	public float totalItemsWeight;
	public String logPrefix;
	public static final String fixedPref = "KnapsackStatusData";

	public KnapsackStatusData() 
	{
    	subject = null;		//subject from input to pass in output
    	message_in = null; 	//message received
    	message_out = null;	//message published
    	knapsackCapacity = -1;			//knapsack capacity
    	packedWeight = -1;		//weight of packed items
    	items = null;	//items received
    	totalItemsWeight = 0; //total weight of all items
    	logPrefix = fixedPref+"<" + System.currentTimeMillis()+ ">: ";
	}
	public String wrapLogMessage( String message)
	{
		return logPrefix + message;
	}
}