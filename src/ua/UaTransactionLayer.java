package ua;

import java.io.IOException;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Timer;
import java.util.TimerTask;

import mensajesSIP.ACKMessage;
import mensajesSIP.BusyHereMessage;
import mensajesSIP.ByeMessage;
import mensajesSIP.InviteMessage;
import mensajesSIP.NotFoundMessage;
import mensajesSIP.OKMessage;
import mensajesSIP.RegisterMessage;
import mensajesSIP.RequestTimeoutMessage;
import mensajesSIP.RingingMessage;
import mensajesSIP.SDPMessage;
import mensajesSIP.SIPMessage;
import mensajesSIP.ServiceUnavailableMessage;
import mensajesSIP.TryingMessage;

public class UaTransactionLayer {
	// Estados de la transaccion INVITE (llamante o llamado)
	private static final int IDLE = 0;
	private static final int CALLING = 1;
	private static final int PROCEEDING = 2;
	private static final int COMPLETED = 3;
	private static final int TERMINATED = 4;
	private static final String[] STATE_NAMES = { "IDLE", "CALLING", "PROCEEDING", "COMPLETED", "TERMINATED" };
	private static final int REGISTER_RETRY_MS = 2000;
	private static final int RETRANSMIT_MS = 200;
	private static final int TERMINATE_MS = 1000;
	private int state = IDLE;

	private UaUserLayer userLayer;
	private UaTransportLayer transportLayer;

	// Transaccion INVITE actual
	private boolean caller; // true: somos el llamante; false: somos el llamado
	private InviteMessage invite; // INVITE enviado (llamante) o recibido (llamado)
	private SIPMessage lastResponse; // llamado: ultima respuesta final enviada (se retransmite cada 200 ms)
	private Timer retransmitTimer;
	private Timer terminateTimer;

	// Transaccion REGISTER (independiente de la del INVITE, para que el
	// re-registro periodico no interfiera con una llamada en curso)
	private boolean registering = false;
	private RegisterMessage lastRegister; // ultimo REGISTER enviado (para reenviarlo)
	private Timer retryTimer;

	public UaTransactionLayer(int listenPort, String proxyAddress, int proxyPort, boolean debug,
			UaUserLayer userLayer) throws SocketException {
		this.userLayer = userLayer;
		this.transportLayer = new UaTransportLayer(listenPort, proxyAddress, proxyPort, debug, this);
	}

	public synchronized void onMessageReceived(SIPMessage sipMessage) throws IOException {
		if (sipMessage instanceof InviteMessage) {
			handleInvite((InviteMessage) sipMessage);
		} else if (sipMessage instanceof ACKMessage) {
			handleACKMessage((ACKMessage) sipMessage);
		} else if (sipMessage instanceof ByeMessage) {
			userLayer.onByeReceived((ByeMessage) sipMessage);
		} else if (sipMessage instanceof TryingMessage) {
			TryingMessage trying = (TryingMessage) sipMessage;
			handleProvisional(trying.getCallId(), trying.getcSeqStr(), "100 Trying");
		} else if (sipMessage instanceof RingingMessage) {
			RingingMessage ringing = (RingingMessage) sipMessage;
			handleProvisional(ringing.getCallId(), ringing.getcSeqStr(), "180 Ringing");
		} else if (sipMessage instanceof OKMessage) {
			handleOKMessage((OKMessage) sipMessage);
		} else if (sipMessage instanceof NotFoundMessage) {
			handleNotFoundMessage((NotFoundMessage) sipMessage);
		} else if (sipMessage instanceof RequestTimeoutMessage) {
			RequestTimeoutMessage timeout = (RequestTimeoutMessage) sipMessage;
			handleFailure(timeout.getCallId(), timeout.getcSeqStr(), timeout.getToName(), timeout.getToUri(),
					"408 Request Timeout");
		} else if (sipMessage instanceof BusyHereMessage) {
			BusyHereMessage busy = (BusyHereMessage) sipMessage;
			handleFailure(busy.getCallId(), busy.getcSeqStr(), busy.getToName(), busy.getToUri(),
					"486 Busy Here");
		} else if (sipMessage instanceof ServiceUnavailableMessage) {
			ServiceUnavailableMessage unavailable = (ServiceUnavailableMessage) sipMessage;
			handleFailure(unavailable.getCallId(), unavailable.getcSeqStr(), unavailable.getToName(),
					unavailable.getToUri(), "503 Service Unavailable");
		} else {
			System.err.println("Unexpected message, throwing away");
		}
	}

	public void startListeningNetwork() {
		transportLayer.startListening();
	}

	/** Envio directo de un mensaje (BYE, 200 OK al BYE...) a un UA, sin pasar por el proxy. */
	public void send(SIPMessage sipMessage, String address, int port) throws IOException {
		transportLayer.send(sipMessage, address, port);
	}

	private void setState(int newState) {
		if (state != newState) {
			System.out.println("Transaccion INVITE: " + STATE_NAMES[state] + " -> " + STATE_NAMES[newState]);
			state = newState;
		}
	}

	// ------------------------------------------------------------------
	// INVITE - lado llamante
	// ------------------------------------------------------------------

	public synchronized void call(InviteMessage inviteMessage) throws IOException {
		stopTimers();
		caller = true;
		invite = inviteMessage;
		lastResponse = null;
		setState(CALLING);
		transportLayer.sendToProxy(inviteMessage);
	}

	private boolean isCurrentCall(String callId, String cSeqStr) {
		return caller && invite != null && "INVITE".equals(cSeqStr) && callId.equals(invite.getCallId());
	}

	// 100 Trying / 180 Ringing
	private void handleProvisional(String callId, String cSeqStr, String description) {
		if (!isCurrentCall(callId, cSeqStr) || (state != CALLING && state != PROCEEDING)) {
			System.err.println("Unexpected message, throwing away");
			return;
		}
		setState(PROCEEDING);
		userLayer.onProvisionalReceived(description);
	}

	// 404 / 408 / 486 / 503 a nuestro INVITE: se envia ACK al proxy
	private void handleFailure(String callId, String cSeqStr, String toName, String toUri, String description)
			throws IOException {
		if (!isCurrentCall(callId, cSeqStr)) {
			System.err.println("Unexpected message, throwing away");
			return;
		}
		switch (state) {
		case CALLING:
		case PROCEEDING:
			sendAckToProxy(toName, toUri);
			setState(COMPLETED);
			startTerminateTimer();
			userLayer.onCallFailed(description);
			break;
		case COMPLETED:
			// Retransmision de la respuesta: se vuelve a enviar el ACK
			sendAckToProxy(toName, toUri);
			break;
		default:
			System.err.println("Unexpected message, throwing away");
			break;
		}
	}

	private ACKMessage buildAck(String toName, String toUri) {
		ACKMessage ack = new ACKMessage();
		ack.setDestination(invite.getDestination());
		ack.setVias(new ArrayList<String>(invite.getVias()));
		ack.setMaxForwards(70);
		ack.setToName(toName);
		ack.setToUri(toUri);
		ack.setFromName(invite.getFromName());
		ack.setFromUri(invite.getFromUri());
		ack.setCallId(invite.getCallId());
		ack.setcSeqNumber(invite.getcSeqNumber()); // el ACK conserva el CSeq del INVITE
		ack.setcSeqStr("ACK");
		ack.setContentLength(0);
		return ack;
	}

	private void sendAckToProxy(String toName, String toUri) throws IOException {
		transportLayer.sendToProxy(buildAck(toName, toUri));
	}

	// ------------------------------------------------------------------
	// INVITE - lado llamado
	// ------------------------------------------------------------------

	private void handleInvite(InviteMessage inviteMessage) throws IOException {
		boolean transactionActive = state == CALLING || state == PROCEEDING || state == COMPLETED;
		if (transactionActive || !userLayer.isIdle()) {
			if (invite != null && invite.getCallId().equals(inviteMessage.getCallId())) {
				System.err.println("Unexpected message, throwing away");
				return;
			}
			// Ocupado: 486 sin iniciar una maquina de estados nueva
			System.out.println("INVITE recibido estando ocupado: se responde 486 Busy Here");
			BusyHereMessage busy = new BusyHereMessage();
			busy.setVias(new ArrayList<String>(inviteMessage.getVias()));
			busy.setToName(inviteMessage.getToName());
			busy.setToUri(inviteMessage.getToUri());
			busy.setFromName(inviteMessage.getFromName());
			busy.setFromUri(inviteMessage.getFromUri());
			busy.setCallId(inviteMessage.getCallId());
			busy.setcSeqNumber(inviteMessage.getcSeqNumber());
			busy.setcSeqStr(inviteMessage.getcSeqStr());
			busy.setContentLength(0);
			transportLayer.sendToProxy(busy);
			return;
		}
		stopTimers();
		caller = false;
		invite = inviteMessage;
		lastResponse = null;
		setState(PROCEEDING);
		// El UA llamado no envia 100 Trying (lo envia el proxy)
		userLayer.onInviteReceived(inviteMessage);
	}

	/** 180 Ringing desde el TU del llamado. */
	public synchronized void sendRinging() throws IOException {
		if (caller || state != PROCEEDING) {
			return;
		}
		RingingMessage ringing = new RingingMessage();
		ringing.setVias(new ArrayList<String>(invite.getVias()));
		ringing.setToName(invite.getToName());
		ringing.setToUri(invite.getToUri());
		ringing.setFromName(invite.getFromName());
		ringing.setFromUri(invite.getFromUri());
		ringing.setCallId(invite.getCallId());
		ringing.setcSeqNumber(invite.getcSeqNumber());
		ringing.setcSeqStr(invite.getcSeqStr());
		ringing.setContact(userLayer.getContact());
		ringing.setContentLength(0);
		transportLayer.sendToProxy(ringing);
	}

	/** 200 OK desde el TU del llamado (con SDP). La transaccion pasa a TERMINATED. */
	public synchronized void sendOK(SDPMessage sdpMessage) throws IOException {
		if (caller || state != PROCEEDING) {
			return;
		}
		OKMessage ok = new OKMessage();
		ok.setVias(new ArrayList<String>(invite.getVias()));
		ok.setToName(invite.getToName());
		ok.setToUri(invite.getToUri());
		ok.setFromName(invite.getFromName());
		ok.setFromUri(invite.getFromUri());
		ok.setCallId(invite.getCallId());
		ok.setcSeqNumber(invite.getcSeqNumber());
		ok.setcSeqStr(invite.getcSeqStr());
		ok.setContact(userLayer.getContact());
		ok.setContentLength(sdpMessage.toStringMessage().getBytes().length);
		ok.setSdp(sdpMessage);
		transportLayer.sendToProxy(ok);
		setState(TERMINATED);
	}

	/** 486 Busy Here desde el TU del llamado (el usuario rechaza la llamada). */
	public synchronized void sendBusyHere() throws IOException {
		if (caller || state != PROCEEDING) {
			return;
		}
		BusyHereMessage busy = new BusyHereMessage();
		busy.setVias(new ArrayList<String>(invite.getVias()));
		busy.setToName(invite.getToName());
		busy.setToUri(invite.getToUri());
		busy.setFromName(invite.getFromName());
		busy.setFromUri(invite.getFromUri());
		busy.setCallId(invite.getCallId());
		busy.setcSeqNumber(invite.getcSeqNumber());
		busy.setcSeqStr(invite.getcSeqStr());
		busy.setContentLength(0);
		sendFinalResponse(busy);
	}

	/** 408 Request Timeout desde el TU del llamado (nadie contesta en 10 s). */
	public synchronized void sendRequestTimeout() throws IOException {
		if (caller || state != PROCEEDING) {
			return;
		}
		RequestTimeoutMessage timeout = new RequestTimeoutMessage();
		timeout.setVias(new ArrayList<String>(invite.getVias()));
		timeout.setToName(invite.getToName());
		timeout.setToUri(invite.getToUri());
		timeout.setFromName(invite.getFromName());
		timeout.setFromUri(invite.getFromUri());
		timeout.setCallId(invite.getCallId());
		timeout.setcSeqNumber(invite.getcSeqNumber());
		timeout.setcSeqStr(invite.getcSeqStr());
		timeout.setContentLength(0);
		sendFinalResponse(timeout);
	}

	// Respuesta de error: Completed, se retransmite cada 200 ms hasta el ACK o 1 s
	private void sendFinalResponse(SIPMessage response) throws IOException {
		lastResponse = response;
		transportLayer.sendToProxy(response);
		setState(COMPLETED);
		startRetransmitTimer();
		startTerminateTimer();
	}

	private void handleACKMessage(ACKMessage ack) {
		if (caller || invite == null || !ack.getCallId().equals(invite.getCallId())) {
			System.err.println("Unexpected message, throwing away");
			return;
		}
		if (state == COMPLETED) {
			// ACK a nuestra respuesta de error
			stopTimers();
			setState(TERMINATED);
		} else if (state == TERMINATED) {
			// ACK al 200 OK: llamada confirmada
			userLayer.onAckReceived();
		} else {
			System.err.println("Unexpected message, throwing away");
		}
	}

	// ------------------------------------------------------------------
	// Respuestas 200 OK (INVITE, BYE y REGISTER) y 404
	// ------------------------------------------------------------------

	private void handleOKMessage(OKMessage okMessage) throws IOException {
		if (isRegisterResponse(okMessage.getcSeqStr(), okMessage.getcSeqNumber())) {
			stopRetryTimer();
			registering = false;
			System.out.println("Registro completado: UA registrado.");
			userLayer.onRegisterResponseReceived();
		} else if (isCurrentCall(okMessage.getCallId(), okMessage.getcSeqStr())
				&& (state == CALLING || state == PROCEEDING)) {
			setState(TERMINATED);
			// El ACK al 200 OK va extremo a extremo, al Contact del llamado
			ACKMessage ack = buildAck(okMessage.getToName(), okMessage.getToUri());
			String[] contact = okMessage.getContact().split(":");
			transportLayer.send(ack, contact[0], Integer.parseInt(contact[1]));
			userLayer.onOKReceived(okMessage);
		} else if ("BYE".equals(okMessage.getcSeqStr())) {
			userLayer.onByeOKReceived(okMessage);
		} else {
			System.err.println("Unexpected message, throwing away");
		}
	}

	private void handleNotFoundMessage(NotFoundMessage notFoundMessage) throws IOException {
		if (isRegisterResponse(notFoundMessage.getcSeqStr(), notFoundMessage.getcSeqNumber())) {
			stopRetryTimer();
			registering = false;
			userLayer.onRegisterNotFound();
		} else {
			handleFailure(notFoundMessage.getCallId(), notFoundMessage.getcSeqStr(), notFoundMessage.getToName(),
					notFoundMessage.getToUri(), "404 Not Found");
		}
	}

	// ------------------------------------------------------------------
	// Temporizadores INVITE
	// ------------------------------------------------------------------

	private void startRetransmitTimer() {
		stopRetransmitTimer();
		final Timer timer = new Timer(true);
		retransmitTimer = timer;
		timer.schedule(new TimerTask() {
			@Override
			public void run() {
				onRetransmitTimer(timer);
			}
		}, RETRANSMIT_MS, RETRANSMIT_MS);
	}

	private synchronized void onRetransmitTimer(Timer timer) {
		if (timer != retransmitTimer || state != COMPLETED) {
			return;
		}
		try {
			transportLayer.sendToProxy(lastResponse);
		} catch (IOException e) {
			System.err.println(e.getMessage());
		}
	}

	private void startTerminateTimer() {
		stopTerminateTimer();
		final Timer timer = new Timer(true);
		terminateTimer = timer;
		timer.schedule(new TimerTask() {
			@Override
			public void run() {
				onTerminateTimer(timer);
			}
		}, TERMINATE_MS);
	}

	private synchronized void onTerminateTimer(Timer timer) {
		if (timer != terminateTimer || state != COMPLETED) {
			return;
		}
		stopRetransmitTimer();
		setState(TERMINATED);
	}

	private void stopRetransmitTimer() {
		if (retransmitTimer != null) {
			retransmitTimer.cancel();
			retransmitTimer = null;
		}
	}

	private void stopTerminateTimer() {
		if (terminateTimer != null) {
			terminateTimer.cancel();
			terminateTimer = null;
		}
	}

	private void stopTimers() {
		stopRetransmitTimer();
		stopTerminateTimer();
	}

	// ------------------------------------------------------------------
	// REGISTER
	// ------------------------------------------------------------------

	private boolean isRegisterResponse(String cSeqStr, String cSeqNumber) {
		return registering && "REGISTER".equals(cSeqStr) && lastRegister.getcSeqNumber().equals(cSeqNumber);
	}

	// Envia el REGISTER y lo reenvia cada 2 s hasta recibir respuesta del proxy
	public synchronized void register(RegisterMessage registerMessage) throws IOException {
		stopRetryTimer();
		lastRegister = registerMessage;
		registering = true;
		transportLayer.sendToProxy(registerMessage);
		retryTimer = new Timer(true);
		retryTimer.schedule(new TimerTask() {
			@Override
			public void run() {
				retryRegister();
			}
		}, REGISTER_RETRY_MS, REGISTER_RETRY_MS);
	}

	private synchronized void retryRegister() {
		if (!registering) {
			return;
		}
		try {
			transportLayer.sendToProxy(lastRegister);
		} catch (IOException e) {
			System.err.println(e.getMessage());
		}
	}

	private void stopRetryTimer() {
		if (retryTimer != null) {
			retryTimer.cancel();
			retryTimer = null;
		}
	}
}
