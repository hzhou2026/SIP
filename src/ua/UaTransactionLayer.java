package ua;

import java.io.IOException;
import java.net.SocketException;
import java.util.Timer;
import java.util.TimerTask;

import mensajesSIP.InviteMessage;
import mensajesSIP.NotFoundMessage;
import mensajesSIP.OKMessage;
import mensajesSIP.RegisterMessage;
import mensajesSIP.SIPMessage;

public class UaTransactionLayer {
	private static final int IDLE = 0;
	private static final int REGISTERING = 1;
	private static final int REGISTER_RETRY_MS = 2000;
	private int state = IDLE;

	private RegisterMessage lastRegister; // ultimo REGISTER enviado (para reenviarlo)
	private Timer retryTimer;

	private UaUserLayer userLayer;
	private UaTransportLayer transportLayer;

	public UaTransactionLayer(int listenPort, String proxyAddress, int proxyPort, boolean debug,
			UaUserLayer userLayer) throws SocketException {
		this.userLayer = userLayer;
		this.transportLayer = new UaTransportLayer(listenPort, proxyAddress, proxyPort, debug, this);
	}

	public void onMessageReceived(SIPMessage sipMessage) throws IOException {
		if (sipMessage instanceof InviteMessage) {
			InviteMessage inviteMessage = (InviteMessage) sipMessage;
			switch (state) {
			case IDLE:
				userLayer.onInviteReceived(inviteMessage);
				break;
			default:
				System.err.println("Unexpected message, throwing away");
				break;
			}
		} else if (sipMessage instanceof OKMessage) {
			handleOKMessage((OKMessage) sipMessage);
		} else if (sipMessage instanceof NotFoundMessage) {
			handleNotFoundMessage((NotFoundMessage) sipMessage);
		} else {
			System.err.println("Unexpected message, throwing away");
		}
	}

	// Respuesta 200 OK: por ahora solo la del REGISTER
	private synchronized void handleOKMessage(OKMessage okMessage) {
		if (isRegisterResponse(okMessage.getcSeqStr(), okMessage.getcSeqNumber())) {
			stopRetryTimer();
			state = IDLE;
			System.out.println("Registro completado: UA registrado.");
			userLayer.onRegisterResponseReceived();
		} else {
			System.err.println("Unexpected message, throwing away");
		}
	}

	// Respuesta 404: por ahora solo la del REGISTER
	private synchronized void handleNotFoundMessage(NotFoundMessage notFoundMessage) {
		if (isRegisterResponse(notFoundMessage.getcSeqStr(), notFoundMessage.getcSeqNumber())) {
			stopRetryTimer();
			state = IDLE;
			userLayer.onRegisterNotFound();
		} else {
			System.err.println("Unexpected message, throwing away");
		}
	}

	private boolean isRegisterResponse(String cSeqStr, String cSeqNumber) {
		return state == REGISTERING && "REGISTER".equals(cSeqStr) && lastRegister.getcSeqNumber().equals(cSeqNumber);
	}

	// Envia el REGISTER y lo reenvia cada 2 s hasta recibir respuesta del proxy
	public synchronized void register(RegisterMessage registerMessage) throws IOException {
		stopRetryTimer();
		lastRegister = registerMessage;
		state = REGISTERING;
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
		if (state != REGISTERING) {
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

	public void startListeningNetwork() {
		transportLayer.startListening();
	}

	public void call(InviteMessage inviteMessage) throws IOException {
		transportLayer.sendToProxy(inviteMessage);
	}
}