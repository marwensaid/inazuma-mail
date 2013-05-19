package rest;

import java.util.UUID;

import javax.annotation.PostConstruct;
import javax.ejb.ConcurrencyManagement;
import javax.ejb.ConcurrencyManagementType;
import javax.ejb.EJB;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import model.SerializedMail;
import controller.MailController;
import database.ConnectionManager;

@Startup
@Singleton
@Path("/")
@ConcurrencyManagement(ConcurrencyManagementType.BEAN)
public class MailService
{
	private MailController mailController;
	
	@EJB
	private ConnectionManager connectionManager;
	
	@PostConstruct
	protected void init()
	{
		mailController = new MailController(connectionManager.getConnection(), 10, 5);
	}
	
	@GET
	@Path("/status")
	@Produces(MediaType.TEXT_PLAIN)
	public String getStatus()
	{
		return "OK";
	}

	@GET
	@Path("/mails/{receiverID}")
	@Produces(MediaType.APPLICATION_JSON)
	public String getMailKeys(@PathParam("receiverID") int receiverID)
	{
		return mailController.getMailKeys(receiverID);
	}

	@GET
	@Path("/mail/{mailKey}")
	@Produces(MediaType.APPLICATION_JSON)
	public String getMail(@PathParam("mailKey") String mailKey)
	{
		return mailController.getMail(mailKey);
	}
	
	@POST
	@Path("/mail/{receiverID}")
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	@Produces(MediaType.TEXT_PLAIN)
	public String putMail(@PathParam("receiverID") int receiverID, @FormParam("document") String document, @FormParam("created") long created)
	{
		final SerializedMail serializedMail = new SerializedMail(receiverID, created, UUID.randomUUID().toString(), document);
		mailController.addMail(serializedMail);
		return serializedMail.getKey();
	}

	@DELETE
	@Path("/mail/{receiverID}/{mailKey}")
	@Produces(MediaType.APPLICATION_JSON)
	public void deleteMail(@PathParam("receiverID") int receiverID, @PathParam("mailKey") String mailKey)
	{
		mailController.deleteMail(receiverID, mailKey);
	}
}
