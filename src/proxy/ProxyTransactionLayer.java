package proxy;

import java.io.IOException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Timer;
import java.util.TimerTask;

import common.FindMyIPv4;
import mensajesSIP.ACKMessage;
import mensajesSIP.BusyHereMessage;
import mensajesSIP.InviteMessage;
import mensajesSIP.NotFoundMessage;
import mensajesSIP.OKMessage;
import mensajesSIP.RegisterMessage;
import mensajesSIP.RequestTimeoutMessage;
import mensajesSIP.RingingMessage;
import mensajesSIP.SIPMessage;
import mensajesSIP.ServiceUnavailableMessage;
import mensajesSIP.TryingMessage;

public class ProxyTransactionLayer {
	// El proxy procesa cada INVITE en dos transacciones:
	// con A (llamante, lado servidor) y con B (llamado, lado cliente)
	private static final int IDLE = 0;
	private static final int CALLING = 1;
	private static final int PROCEEDING = 2;
	private static final int COMPLETED = 3;
	private static final int TERMINATED = 4;
	private static final String[] STATE_NAMES = { "IDLE", "CALLING", "PROCEEDING", "COMPLETED", "TERMINATED" };
	private static final int RETRANSMIT_MS = 200;
	private static final int TERMINATE_MS = 1000;
	private int stateA = IDLE;
	private int stateB = IDLE;

	private ProxyUserLayer userLayer;
	private ProxyTransportLayer transportLayer;
	private String myVia;

	// Llamada en curso (callId == null: el proxy esta libre)
	private String callId = null;
	private String addressA;
	private int portA;
	private String addressB;
	private int portB;
	private InviteMessage inviteB; // INVITE reenviado a B (para construir el ACK si B responde con error)
	private SIPMessage lastResponseA; // respuesta de error enviada a A (se retransmite cada 200 ms)
	private Timer retransmitTimerA;
	private Timer terminateTimerA;
	private Timer terminateTimerB;

	public ProxyTransactionLayer(int listenPort, boolean debug, ProxyUserLayer userLayer)
			throws SocketException, UnknownHostException {
		this.userLayer = userLayer;
		this.transportLayer = new ProxyTransportLayer(listenPort, debug, this);
		this.myVia = FindMyIPv4.findMyIPv4Address().getHostAddress() + ":" + listenPort;
	}

	public synchronized void onMessageReceived(SIPMessage sipMessage) throws IOException {
		if (sipMessage instanceof RegisterMessage) {
			userLayer.onRegisterReceived((RegisterMessage) sipMessage);
		} else if (sipMessage instanceof InviteMessage) {
			handleInvite((InviteMessage) sipMessage);
		} else if (sipMessage instanceof ACKMessage) {
			handleACKMessage((ACKMessage) sipMessage);
		} else if (sipMessage instanceof RingingMessage) {
			handleRinging((RingingMessage) sipMessage);
		} else if (sipMessage instanceof OKMessage) {
			handleOKMessage((OKMessage) sipMessage);
		} else if (sipMessage instanceof NotFoundMessage) {
			NotFoundMessage m = (NotFoundMessage) sipMessage;
			handleFailureFromB(m, m.getVias(), m.getCallId(), m.getcSeqStr(), m.getToName(), m.getToUri());
		} else if (sipMessage instanceof RequestTimeoutMessage) {
			RequestTimeoutMessage m = (RequestTimeoutMessage) sipMessage;
			handleFailureFromB(m, m.getVias(), m.getCallId(), m.getcSeqStr(), m.getToName(), m.getToUri());
		} else if (sipMessage instanceof BusyHereMessage) {
			BusyHereMessage m = (BusyHereMessage) sipMessage;
			handleFailureFromB(m, m.getVias(), m.getCallId(), m.getcSeqStr(), m.getToName(), m.getToUri());
		} else if (sipMessage instanceof ServiceUnavailableMessage) {
			ServiceUnavailableMessage m = (ServiceUnavailableMessage) sipMessage;
			handleFailureFromB(m, m.getVias(), m.getCallId(), m.getcSeqStr(), m.getToName(), m.getToUri());
		} else {
			System.err.println("Unexpected message, throwing away");
		}
	}

	public void send(SIPMessage sipMessage, String address, int port) throws IOException {
		transportLayer.send(sipMessage, address, port);
	}

	public void startListening() {
		transportLayer.startListening();
	}

	private void setStateA(int newState) {
		if (stateA != newState) {
			System.out.println("[A] Transaccion con A (llamante): " + STATE_NAMES[stateA] + " -> "
					+ STATE_NAMES[newState]);
			stateA = newState;
		}
	}

	private void setStateB(int newState) {
		if (stateB != newState) {
			System.out.println("[B] Transaccion con B (llamado): " + STATE_NAMES[stateB] + " -> "
					+ STATE_NAMES[newState]);
			stateB = newState;
		}
	}

	private boolean isCurrentInviteResponse(String messageCallId, String cSeqStr) {
		return callId != null && callId.equals(messageCallId) && "INVITE".equals(cSeqStr);
	}

	// El proxy queda libre al terminar la transaccion con A (sin loose-routing)
	private void callFinished() {
		callId = null;
		System.out.println("Llamada finalizada en el proxy: proxy libre");
	}

	// ------------------------------------------------------------------
	// INVITE de A
	// ------------------------------------------------------------------

	private void handleInvite(InviteMessage invite) throws IOException {
		String[] origin = invite.getVias().get(0).split(":");

		if (callId != null) {
			if (callId.equals(invite.getCallId())) {
				System.err.println("Unexpected message, throwing away");
				return;
			}
			// Proxy ocupado: 503 sin generar una transaccion nueva
			System.out.println("Proxy ocupado: INVITE con distinto Call-ID -> 503 Service Unavailable");
			ServiceUnavailableMessage unavailable = new ServiceUnavailableMessage();
			unavailable.setVias(new ArrayList<String>(invite.getVias()));
			unavailable.setToName(invite.getToName());
			unavailable.setToUri(invite.getToUri());
			unavailable.setFromName(invite.getFromName());
			unavailable.setFromUri(invite.getFromUri());
			unavailable.setCallId(invite.getCallId());
			unavailable.setcSeqNumber(invite.getcSeqNumber());
			unavailable.setcSeqStr(invite.getcSeqStr());
			unavailable.setContentLength(0);
			transportLayer.send(unavailable, origin[0], Integer.parseInt(origin[1]));
			return;
		}

		// Llamada nueva
		stopTimers();
		stateA = IDLE;
		stateB = IDLE;
		lastResponseA = null;
		inviteB = null;
		callId = invite.getCallId();
		addressA = origin[0];
		portA = Integer.parseInt(origin[1]);
		setStateA(PROCEEDING);

		// El proxy envia 100 Trying al llamante
		TryingMessage trying = new TryingMessage();
		trying.setVias(new ArrayList<String>(invite.getVias()));
		trying.setToName(invite.getToName());
		trying.setToUri(invite.getToUri());
		trying.setFromName(invite.getFromName());
		trying.setFromUri(invite.getFromUri());
		trying.setCallId(invite.getCallId());
		trying.setcSeqNumber(invite.getcSeqNumber());
		trying.setcSeqStr(invite.getcSeqStr());
		trying.setContentLength(0);
		transportLayer.send(trying, addressA, portA);

		userLayer.onInviteReceived(invite);
	}

	/** El TU decide reenviar el INVITE a B (ambos registrados). */
	public synchronized void forwardInviteToB(InviteMessage invite, String address, int port) throws IOException {
		addressB = address;
		portB = port;
		invite.addVia(myVia);
		invite.setMaxForwards(invite.getMaxForwards() - 1);
		inviteB = invite;
		setStateB(CALLING);
		transportLayer.send(invite, addressB, portB);
	}

	/** El TU responde con un error (404) al llamante: Completed, se espera el ACK. */
	public synchronized void sendFailureToA(SIPMessage response) throws IOException {
		lastResponseA = response;
		transportLayer.send(response, addressA, portA);
		setStateA(COMPLETED);
		startRetransmitTimerA();
		startTerminateTimerA();
	}

	// ------------------------------------------------------------------
	// Respuestas de B (se reenvian a A quitando la Via del proxy)
	// ------------------------------------------------------------------

	private void handleRinging(RingingMessage ringing) throws IOException {
		if (!isCurrentInviteResponse(ringing.getCallId(), ringing.getcSeqStr())
				|| (stateB != CALLING && stateB != PROCEEDING)) {
			System.err.println("Unexpected message, throwing away");
			return;
		}
		setStateB(PROCEEDING);
		ringing.getVias().remove(0);
		transportLayer.send(ringing, addressA, portA);
	}

	private void handleOKMessage(OKMessage ok) throws IOException {
		if (!isCurrentInviteResponse(ok.getCallId(), ok.getcSeqStr())
				|| (stateB != CALLING && stateB != PROCEEDING)) {
			System.err.println("Unexpected message, throwing away");
			return;
		}
		// 200 OK: ambas transacciones terminan y el proxy queda libre.
		// El ACK y el BYE iran extremo a extremo (sin loose-routing).
		setStateB(TERMINATED);
		ok.getVias().remove(0);
		transportLayer.send(ok, addressA, portA);
		setStateA(TERMINATED);
		stopTimers();
		callFinished();
	}

	// 404 / 408 / 486 / 503 de B
	private void handleFailureFromB(SIPMessage response, ArrayList<String> vias, String messageCallId,
			String cSeqStr, String toName, String toUri) throws IOException {
		if (!isCurrentInviteResponse(messageCallId, cSeqStr)) {
			System.err.println("Unexpected message, throwing away");
			return;
		}
		if (stateB == CALLING || stateB == PROCEEDING) {
			sendAckToB(toName, toUri);
			setStateB(COMPLETED);
			startTerminateTimerB();
			vias.remove(0);
			sendFailureToA(response);
		} else if (stateB == COMPLETED) {
			// Retransmision de B: solo se repite el ACK
			sendAckToB(toName, toUri);
		} else {
			System.err.println("Unexpected message, throwing away");
		}
	}

	private void sendAckToB(String toName, String toUri) throws IOException {
		ACKMessage ack = new ACKMessage();
		ack.setDestination(inviteB.getDestination());
		ack.setVias(new ArrayList<String>(Arrays.asList(myVia)));
		ack.setMaxForwards(70);
		ack.setToName(toName);
		ack.setToUri(toUri);
		ack.setFromName(inviteB.getFromName());
		ack.setFromUri(inviteB.getFromUri());
		ack.setCallId(inviteB.getCallId());
		ack.setcSeqNumber(inviteB.getcSeqNumber());
		ack.setcSeqStr("ACK");
		ack.setContentLength(0);
		transportLayer.send(ack, addressB, portB);
	}

	// ------------------------------------------------------------------
	// ACK de A (a una respuesta de error)
	// ------------------------------------------------------------------

	private void handleACKMessage(ACKMessage ack) {
		if (callId != null && callId.equals(ack.getCallId()) && stateA == COMPLETED) {
			stopRetransmitTimerA();
			stopTerminateTimerA();
			setStateA(TERMINATED);
			callFinished();
		} else {
			System.err.println("Unexpected message, throwing away");
		}
	}

	// ------------------------------------------------------------------
	// Temporizadores
	// ------------------------------------------------------------------

	private void startRetransmitTimerA() {
		stopRetransmitTimerA();
		final Timer timer = new Timer(true);
		retransmitTimerA = timer;
		timer.schedule(new TimerTask() {
			@Override
			public void run() {
				onRetransmitTimerA(timer);
			}
		}, RETRANSMIT_MS, RETRANSMIT_MS);
	}

	private synchronized void onRetransmitTimerA(Timer timer) {
		if (timer != retransmitTimerA || stateA != COMPLETED) {
			return;
		}
		try {
			transportLayer.send(lastResponseA, addressA, portA);
		} catch (IOException e) {
			System.err.println(e.getMessage());
		}
	}

	private void startTerminateTimerA() {
		stopTerminateTimerA();
		final Timer timer = new Timer(true);
		terminateTimerA = timer;
		timer.schedule(new TimerTask() {
			@Override
			public void run() {
				onTerminateTimerA(timer);
			}
		}, TERMINATE_MS);
	}

	private synchronized void onTerminateTimerA(Timer timer) {
		if (timer != terminateTimerA || stateA != COMPLETED) {
			return;
		}
		stopRetransmitTimerA();
		setStateA(TERMINATED);
		callFinished();
	}

	private void startTerminateTimerB() {
		stopTerminateTimerB();
		final Timer timer = new Timer(true);
		terminateTimerB = timer;
		timer.schedule(new TimerTask() {
			@Override
			public void run() {
				onTerminateTimerB(timer);
			}
		}, TERMINATE_MS);
	}

	private synchronized void onTerminateTimerB(Timer timer) {
		if (timer != terminateTimerB || stateB != COMPLETED) {
			return;
		}
		setStateB(TERMINATED);
	}

	private void stopRetransmitTimerA() {
		if (retransmitTimerA != null) {
			retransmitTimerA.cancel();
			retransmitTimerA = null;
		}
	}

	private void stopTerminateTimerA() {
		if (terminateTimerA != null) {
			terminateTimerA.cancel();
			terminateTimerA = null;
		}
	}

	private void stopTerminateTimerB() {
		if (terminateTimerB != null) {
			terminateTimerB.cancel();
			terminateTimerB = null;
		}
	}

	private void stopTimers() {
		stopRetransmitTimerA();
		stopTerminateTimerA();
		stopTerminateTimerB();
	}
}
