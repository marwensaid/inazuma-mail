package queue;

public class SerializedMail
{
	private final int receiverID;
	private final long created;
	private final String key;
	private final String document;

	public SerializedMail(final int receiverID, final long created, final String key, final String document)
	{
		this.receiverID = receiverID;
		this.created = created;
		this.key = key;
		this.document = document;
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
}
