package com.sjtools.knapsack;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.lambda.runtime.LambdaLogger;

public class EagerKnapsackPacker 
{
	private int capacity;
	private List<Integer> items;
	private float totalItemWeight;
	private float packed_capacity;
	private Map<Integer,Integer> packed_items;
	
	public EagerKnapsackPacker(List<Integer> items, float totalItemWeight, int capacity) 
	{
		this.items = items;
		this.capacity = capacity;
		this.totalItemWeight = totalItemWeight;
		this.packed_items = new LinkedHashMap<Integer,Integer>();
		this.packed_capacity = 0;
	}
	void pack(LambdaLogger logger, boolean isClassic)
	{
		float remaining_weight=totalItemWeight;
		int prev_packed_idx = -1;
		int cur_idx = -1;
		Iterator<Integer> it = this.items.iterator();
		while(it.hasNext())
		{
			cur_idx++;
			int item = it.next();
			remaining_weight-=item;
			//skip item that does not fit into knapsack at all
			if (item > capacity)
				continue;
			//if item is of size of a knapsack, then this is solution already
			if (item == capacity)
			{
				packed_capacity = capacity;
				packed_items.clear();
				packed_items.put(cur_idx, item);
				return;
			}
			if ((packed_capacity + item) <= capacity)
			{//item can be packed
				packed_capacity+=item;
				packed_items.put(cur_idx, item);
				prev_packed_idx = cur_idx;
				//full knapsack, solution found
				if (packed_capacity==capacity)
				{
					return;
				}
			}
			else
			{
				//item cannot be put into knapsack

				//remove last packed one, if sum packed without that last one plus remaining still exceeds capacity
				//put current instead
				
				if (!isClassic && (packed_capacity-packed_items.get(prev_packed_idx) + remaining_weight + item >=capacity))
				{
					logger.log("Skipping prev_item =" + packed_items.get(prev_packed_idx) +
									" packed_capa= " + packed_capacity  + 
									" remaining_w= " + remaining_weight +
									" item= " + item +									
									" capacity= " + capacity);
					packed_capacity-=packed_items.get(prev_packed_idx);
					packed_items.remove(prev_packed_idx);
					packed_capacity+=item;
					packed_items.put(cur_idx, item);
					prev_packed_idx = cur_idx;
				}
				else
				{
					//just skip this item
				}
			}
		}
	}
	
	public int getPackedWeight()
	{
		return (int)packed_capacity;
	}
	public List<Integer> getPackedItems()
	{
		List<Integer> res = new LinkedList<Integer>();
		packed_items.forEach((k,v)-> res.add(v));
		return res;
	}
}