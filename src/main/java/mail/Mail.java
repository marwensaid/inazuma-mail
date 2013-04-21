package mail;

import java.util.UUID;

public abstract class Mail
{
	protected final transient UUID key;
	protected final Types mailType;
	protected final int senderID;
	protected final int receiverID;
	protected final long created;
	
	protected Mail(final Types mailType, final int senderID, final int receiverID)
	{
		this.key = UUID.randomUUID();
		this.mailType = mailType;
		this.senderID = senderID;
		this.receiverID = receiverID;
		this.created = System.nanoTime();
		//System.out.println("Created mail_" + key);
	}
	
	public String getKey()
	{
		return key.toString();
	}

	public Types getMailType()
	{
		return mailType;
	}

	public int getSenderID()
	{
		return senderID;
	}

	public int getReceiverID()
	{
		return receiverID;
	}

	public long getCreated()
	{
		return created;
	}
}
