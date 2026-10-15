package proxy;

import java.io.IOException;
import java.net.SocketException;
import java.util.ArrayList;
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
	private static final List<String> VALID_USERS = Arrays.asList("alice@sma", "bob@sma");

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

	public ProxyUserLayer(int listenPort, boolean debug) throws SocketException {
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

	public void onInviteReceived(InviteMessage inviteMessage) throws IOException {
		System.out.println("Received INVITE from " + inviteMessage.getFromName());
		ArrayList<String> vias = inviteMessage.getVias();
		String origin = vias.get(0);
		String[] originParts = origin.split(":");
		String originAddress = originParts[0];
		int originPort = Integer.parseInt(originParts[1]);
		transactionLayer.echoInvite(inviteMessage, originAddress, originPort);
	}

	public void startListening() {
		transactionLayer.startListening();
	}
}