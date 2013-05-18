package model;

public class SerializedMail implements StatusMessageObject
{
	private final int receiverID;
	private final long created;
	private final String key;
	private final String document;
	
	private int tries;
	private Exception lastException;

	public SerializedMail(final int receiverID, final long created, final String key, final String document)
	{
		this.receiverID = receiverID;
		this.created = created;
		this.key = key;
		this.document = document;
		
		tries = 0;
		lastException = null;
	}

	public int getReceiverID()
	{
		return receiverID;
	}

	public long getCreated()
	{
		return created;
	}

	public String getKey()
	{
		return key;
	}

	public String getDocument()
	{
		return document;
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
