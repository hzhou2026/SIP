package proxy;

import java.io.IOException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mensajesSIP.InviteMessage;
import mensajesSIP.NotFoundMessage;
import mensajesSIP.OKMessage;
import mensajesSIP.RegisterMessage;

public class ProxyUserLayer {
	// Usuarios permitidos (en minusculas: la comparacion no distingue mayusculas)
	private static final List<String> VALID_USERS = Arrays.asList("alice@sma", "bob@sma", "carol@sma");

	// Localizacion de un usuario registrado: contacto (ip:puerto) y marca temporal de fin de registro
	private static class Registration {
		final String contact;
		final long expiresAt;

		Registration(String contact, long expiresAt) {
			this.contact = contact;
			this.expiresAt = expiresAt;
		}
	}

	// Tabla de registro/localizacion. Un nuevo registro de un usuario sustituye al anterior.
	private Map<String, Registration> registrations = new HashMap<String, Registration>();

	private ProxyTransactionLayer transactionLayer;

	public ProxyUserLayer(int listenPort, boolean debug) throws SocketException, UnknownHostException {
		this.transactionLayer = new ProxyTransactionLayer(listenPort, debug, this);
	}

	public void onRegisterReceived(RegisterMessage registerMessage) throws IOException {
		String user = registerMessage.getToUri().substring("sip:".length()).toLowerCase();
		String[] origin = registerMessage.getVias().get(0).split(":");
		String originAddress = origin[0];
		int originPort = Integer.parseInt(origin[1]);

		if (!VALID_USERS.contains(user)) {
			NotFoundMessage notFound = new NotFoundMessage();
			notFound.setVias(registerMessage.getVias());
			notFound.setToName(registerMessage.getToName());
			notFound.setToUri(registerMessage.getToUri());
			notFound.setFromName(registerMessage.getFromName());
			notFound.setFromUri(registerMessage.getFromUri());
			notFound.setCallId(registerMessage.getCallId());
			notFound.setcSeqNumber(registerMessage.getcSeqNumber());
			notFound.setcSeqStr(registerMessage.getcSeqStr());
			notFound.setContact(registerMessage.getContact());
			notFound.setContentLength(0);
			transactionLayer.send(notFound, originAddress, originPort);
			return;
		}

		long expires = Long.parseLong(registerMessage.getExpires());
		registrations.put(user, new Registration(registerMessage.getContact(),
				System.currentTimeMillis() + expires * 1000));

		OKMessage ok = new OKMessage();
		ok.setVias(registerMessage.getVias());
		ok.setToName(registerMessage.getToName());
		ok.setToUri(registerMessage.getToUri());
		ok.setFromName(registerMessage.getFromName());
		ok.setFromUri(registerMessage.getFromUri());
		ok.setCallId(registerMessage.getCallId());
		ok.setcSeqNumber(registerMessage.getcSeqNumber());
		ok.setcSeqStr(registerMessage.getcSeqStr());
		ok.setContact(registerMessage.getContact());
		ok.setContentLength(0);
		transactionLayer.send(ok, originAddress, originPort);
	}

	// Registro vigente (existente y no caducado) del usuario de una URI sip:usuario@dominio
	private Registration findRegistration(String uri) {
		Registration registration = registrations.get(uri.substring("sip:".length()).toLowerCase());
		if (registration != null && registration.expiresAt > System.currentTimeMillis()) {
			return registration;
		}
		return null;
	}

	// INVITE de A: si A y B estan registrados se reenvia a B; si no, 404 a A
	public void onInviteReceived(InviteMessage inviteMessage) throws IOException {
		System.out.println("Received INVITE from " + inviteMessage.getFromName());
		Registration caller = findRegistration(inviteMessage.getFromUri());
		Registration callee = findRegistration(inviteMessage.getToUri());

		if (caller == null || callee == null) {
			System.out.println("Llamante o llamado no registrado: se responde 404 Not Found");
			NotFoundMessage notFound = new NotFoundMessage();
			notFound.setVias(inviteMessage.getVias());
			notFound.setToName(inviteMessage.getToName());
			notFound.setToUri(inviteMessage.getToUri());
			notFound.setFromName(inviteMessage.getFromName());
			notFound.setFromUri(inviteMessage.getFromUri());
			notFound.setCallId(inviteMessage.getCallId());
			notFound.setcSeqNumber(inviteMessage.getcSeqNumber());
			notFound.setcSeqStr(inviteMessage.getcSeqStr());
			notFound.setContact(inviteMessage.getContact());
			notFound.setContentLength(0);
			transactionLayer.sendFailureToA(notFound);
			return;
		}

		String[] contact = callee.contact.split(":");
		transactionLayer.forwardInviteToB(inviteMessage, contact[0], Integer.parseInt(contact[1]));
	}

	public void startListening() {
		transactionLayer.startListening();
	}
}
