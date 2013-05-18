package model;

import java.lang.reflect.Type;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class ReceiverLookupDocument implements StatusMessageObject
{
	private final static Type typeOfMap = new TypeToken<ConcurrentHashMap<String, Long>>() {}.getType();
	
	private final ConcurrentMap<String, Long> lookup;
	
	private int tries = 0;
	private Exception lastException = null;

	public ReceiverLookupDocument()
	{
		lookup = new ConcurrentHashMap<String, Long>();
	}
	
	public ReceiverLookupDocument(final String json)
	{
		lookup = new Gson().fromJson(json, typeOfMap);
	}

	public boolean add(final long created, final String mailKey)
	{
		if (lookup.putIfAbsent(mailKey, created) != null)
		{
			return false;
		}
		return true;
	}
	
	public void remove(final String mailKey)
	{
		lookup.remove(mailKey);
	}
	
	public int size()
	{
		return lookup.size();
	}

	public Set<String> keySet()
	{
		return lookup.keySet();
	}

	public String toJSON()
	{
		Gson gson = new Gson();
		return gson.toJson(lookup);
	}

	public static ReceiverLookupDocument fromJSON(final String value)
	{
		return new ReceiverLookupDocument(value);
	}
	
	@Override
	public int getTries()
	{
		return tries;
	}

	@Override
	public void incrementTries()
	{
		this.tries++;
	}

	@Override
	public Exception getLastException()
	{
		return lastException;
	}

	@Override
	public void setLastException(Exception lastException)
	{
		this.lastException = lastException;
	}

	@Override
	public void resetStatus()
	{
		tries = 0;
		lastException = null;
	}
}
