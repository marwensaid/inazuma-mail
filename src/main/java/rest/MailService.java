package rest;

import java.util.UUID;

import javax.annotation.PostConstruct;
import javax.ejb.ConcurrencyManagement;
import javax.ejb.ConcurrencyManagementType;
import javax.ejb.EJB;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.ws.rs.Consumes;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import mailstorage.MailStorage;
import mailstorage.SerializedMail;
import database.ConnectionManager;

@Path("/")
@Startup
@Singleton
@ConcurrencyManagement(ConcurrencyManagementType.BEAN)
public class MailService
{
	private MailStorage mailStorage;
	
	@EJB
	private ConnectionManager connectionManager;

	@PostConstruct
	protected void init()
	{
		mailStorage = new MailStorage(connectionManager.getConnection(), 10, 5);
	}
	
	@GET
	@Path("/status")
	@Produces(MediaType.TEXT_PLAIN)
	public String getStatus()
	{
		return "OK";
	}

	@GET
	@Path("/mail/{mailKey}")
	@Produces(MediaType.APPLICATION_JSON)
	public String getMail(@PathParam("mailKey") String mailKey)
	{
		return mailStorage.getMail(mailKey);
	}
	
	@POST
	@Path("/mail/{receiverID}")
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	@Produces(MediaType.TEXT_PLAIN)
	public String putMail(@PathParam("receiverID") int receiverID, @FormParam("document") String document)
	{
		final SerializedMail serializedMail = new SerializedMail(receiverID, System.currentTimeMillis() / 1000, UUID.randomUUID().toString(), document);
		mailStorage.addMail(serializedMail);
		return serializedMail.getKey();
	}
}
