package ua;

import java.io.IOException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import common.FindMyIPv4;
import mensajesSIP.ByeMessage;
import mensajesSIP.InviteMessage;
import mensajesSIP.OKMessage;
import mensajesSIP.RegisterMessage;
import mensajesSIP.SDPMessage;

public class UaUserLayer {
	// Estado de la llamada
	private static final int IDLE = 0; // fuera de llamada
	private static final int CALLING = 1; // llamante: INVITE enviado, esperando respuesta
	private static final int RINGING = 2; // llamado: sonando, esperando que el usuario conteste
	private static final int ANSWERED = 3; // llamado: 200 OK enviado, esperando el ACK
	private static final int IN_CALL = 4; // llamada activa (ACK o 200 OK recibido)
	private static final int BYE_SENT = 5; // BYE enviado, esperando su 200 OK
	private static final int RING_TIMEOUT_MS = 10000;
	private volatile int state = IDLE;

	public static final ArrayList<Integer> RTPFLOWS = new ArrayList<Integer>(
			Arrays.asList(new Integer[] { 99, 90 }));

	private UaTransactionLayer transactionLayer;

	private String myAddress = FindMyIPv4.findMyIPv4Address().getHostAddress();
	private int rtpPort;
	private int listenPort;

	private Process vitextClient = null;
	private Process vitextServer = null;

	// Datos de registro
	private String userName;
	private String domain;
	private int registerTime; // segundos (cabecera Expires)
	private String registerCallId = UUID.randomUUID().toString();
	private int registerCSeq = 0;
	private Timer registerTimer;
	private CountDownLatch registered = new CountDownLatch(1);

	// Datos de la llamada actual
	private boolean isCaller; // true: llamante; false: llamado
	private String callId;
	private int callCSeq; // CSeq del INVITE; el BYE usa callCSeq + 1
	private String peerName;
	private String peerUri;
	private String peerContact; // ip:puerto del otro extremo (el BYE y el ACK van directos)
	private Timer ringTimer;

	public UaUserLayer(String userName, String domain, int listenPort, String proxyAddress, int proxyPort,
			boolean debug, int registerTime) throws SocketException, UnknownHostException {
		this.userName = userName;
		this.domain = domain;
		this.registerTime = registerTime;
		this.transactionLayer = new UaTransactionLayer(listenPort, proxyAddress, proxyPort, debug, this);
		this.listenPort = listenPort;
		this.rtpPort = listenPort + 1;
	}

	public String getContact() {
		return myAddress + ":" + listenPort;
	}

	public boolean isIdle() {
		return state == IDLE;
	}

	// ------------------------------------------------------------------
	// REGISTER
	// ------------------------------------------------------------------

	// Registra al usuario en el proxy y espera a recibir el 200 OK
	public void register() throws IOException, InterruptedException {
		sendRegister();
		registered.await();
	}

	private void sendRegister() throws IOException {
		RegisterMessage registerMessage = new RegisterMessage();
		registerMessage.setDestination("sip:" + domain);
		registerMessage.setVias(new ArrayList<String>(Arrays.asList(this.myAddress + ":" + this.listenPort)));
		registerMessage.setMaxForwards(70);
		registerMessage.setToName(userName);
		registerMessage.setToUri("sip:" + userName + "@" + domain);
		registerMessage.setFromName(userName);
		registerMessage.setFromUri("sip:" + userName + "@" + domain);
		registerMessage.setCallId(registerCallId);
		registerCSeq++;
		registerMessage.setcSeqNumber(String.valueOf(registerCSeq));
		registerMessage.setcSeqStr("REGISTER");
		registerMessage.setContact(myAddress + ":" + listenPort);
		registerMessage.setExpires(String.valueOf(registerTime));
		registerMessage.setContentLength(0);
		transactionLayer.register(registerMessage);
	}

	// 200 OK al REGISTER: se permiten llamadas y se programa el siguiente registro al expirar este
	public void onRegisterResponseReceived() {
		registered.countDown();
		registerTimer = new Timer(true);
		registerTimer.schedule(new TimerTask() {
			@Override
			public void run() {
				try {
					sendRegister();
				} catch (IOException e) {
					System.err.println(e.getMessage());
				}
			}
		}, registerTime * 1000L);
	}

	// 404 al REGISTER: usuario no permitido, el UA finaliza
	public void onRegisterNotFound() {
		System.out.println("Usuario no registrado en el proxy (404 Not Found). Finalizando.");
		System.exit(1);
	}

	// ------------------------------------------------------------------
	// Llamada entrante (llamado)
	// ------------------------------------------------------------------

	public void onInviteReceived(InviteMessage inviteMessage) throws IOException {
		System.out.println("Received INVITE from " + inviteMessage.getFromName());
		isCaller = false;
		callId = inviteMessage.getCallId();
		callCSeq = Integer.parseInt(inviteMessage.getcSeqNumber());
		peerName = inviteMessage.getFromName();
		peerUri = inviteMessage.getFromUri();
		peerContact = inviteMessage.getContact();
		state = RINGING;
		transactionLayer.sendRinging(); // 180 Ringing
		startRingTimer();
		System.out.println("Llamada entrante de " + peerName + ": teclea S para contestar o N para rechazar");
	}

	private void startRingTimer() {
		cancelRingTimer();
		final Timer timer = new Timer(true);
		ringTimer = timer;
		timer.schedule(new TimerTask() {
			@Override
			public void run() {
				onRingTimeout(timer);
			}
		}, RING_TIMEOUT_MS);
	}

	private void cancelRingTimer() {
		if (ringTimer != null) {
			ringTimer.cancel();
			ringTimer = null;
		}
	}

	// Pasados 10 s sin contestar: 408 Request Timeout
	private void onRingTimeout(Timer timer) {
		synchronized (transactionLayer) {
			if (timer != ringTimer || state != RINGING) {
				return;
			}
			ringTimer = null;
			state = IDLE;
			System.out.println("Llamada no contestada en 10 s: se responde 408 Request Timeout");
			try {
				transactionLayer.sendRequestTimeout();
			} catch (IOException e) {
				System.err.println(e.getMessage());
			}
		}
	}

	private void answerCall(boolean accept) throws IOException {
		cancelRingTimer();
		if (accept) {
			SDPMessage sdpMessage = new SDPMessage();
			sdpMessage.setIp(this.myAddress);
			sdpMessage.setPort(this.rtpPort);
			sdpMessage.setOptions(RTPFLOWS);
			state = ANSWERED;
			transactionLayer.sendOK(sdpMessage);
			runVitextServer();
		} else {
			state = IDLE;
			System.out.println("Llamada rechazada: se responde 486 Busy Here");
			transactionLayer.sendBusyHere();
		}
	}

	// ACK al 200 OK: la llamada queda activa en el llamado
	public void onAckReceived() {
		if (state == ANSWERED) {
			state = IN_CALL;
			System.out.println("ACK recibido: llamada establecida con " + peerName + ". Teclea BYE para finalizarla.");
		}
	}

	// ------------------------------------------------------------------
	// Llamada saliente (llamante)
	// ------------------------------------------------------------------

	// 100 Trying / 180 Ringing
	public void onProvisionalReceived(String description) {
		System.out.println("Recibido " + description);
	}

	// 200 OK al INVITE (el ACK ya lo ha enviado la capa de transaccion)
	public void onOKReceived(OKMessage okMessage) {
		peerContact = okMessage.getContact();
		state = IN_CALL;
		System.out.println("Llamada establecida con " + peerName + ". Teclea BYE para finalizarla.");
		runVitextClient();
	}

	// 404 / 408 / 486 / 503 al INVITE
	public void onCallFailed(String description) {
		state = IDLE;
		System.out.println("Llamada no establecida: " + description);
	}

	// ------------------------------------------------------------------
	// BYE
	// ------------------------------------------------------------------

	private void commandBye() throws IOException {
		if (state != IN_CALL) {
			System.out.println("Error: no hay ninguna llamada activa, BYE no enviado");
			return;
		}
		ByeMessage byeMessage = new ByeMessage();
		byeMessage.setDestination(peerUri);
		byeMessage.setVias(new ArrayList<String>(Arrays.asList(getContact())));
		byeMessage.setMaxForwards(70);
		byeMessage.setToName(peerName);
		byeMessage.setToUri(peerUri);
		byeMessage.setFromName(userName);
		byeMessage.setFromUri("sip:" + userName + "@" + domain);
		byeMessage.setCallId(callId);
		byeMessage.setcSeqNumber(String.valueOf(callCSeq + 1));
		byeMessage.setcSeqStr("BYE");
		byeMessage.setContentLength(0);
		state = BYE_SENT;
		// Sin loose-routing el BYE va extremo a extremo, al Contact del otro UA
		String[] contact = peerContact.split(":");
		transactionLayer.send(byeMessage, contact[0], Integer.parseInt(contact[1]));
	}

	// BYE recibido del otro extremo: se contesta 200 OK y finaliza la llamada
	public void onByeReceived(ByeMessage byeMessage) throws IOException {
		if ((state != IN_CALL && state != ANSWERED && state != BYE_SENT) || !byeMessage.getCallId().equals(callId)) {
			System.err.println("Unexpected message, throwing away");
			return;
		}
		OKMessage ok = new OKMessage();
		ok.setVias(new ArrayList<String>(byeMessage.getVias()));
		ok.setToName(byeMessage.getToName());
		ok.setToUri(byeMessage.getToUri());
		ok.setFromName(byeMessage.getFromName());
		ok.setFromUri(byeMessage.getFromUri());
		ok.setCallId(byeMessage.getCallId());
		ok.setcSeqNumber(byeMessage.getcSeqNumber());
		ok.setcSeqStr(byeMessage.getcSeqStr());
		ok.setContact(getContact());
		ok.setContentLength(0);
		String[] origin = byeMessage.getVias().get(0).split(":");
		transactionLayer.send(ok, origin[0], Integer.parseInt(origin[1]));
		endCall("Llamada finalizada por " + peerName + " (BYE recibido)");
	}

	// 200 OK a nuestro BYE
	public void onByeOKReceived(OKMessage okMessage) {
		if (state != BYE_SENT || !okMessage.getCallId().equals(callId)) {
			System.err.println("Unexpected message, throwing away");
			return;
		}
		endCall("Llamada finalizada (200 OK al BYE recibido)");
	}

	private void endCall(String message) {
		stopVitextServer();
		stopVitextClient();
		state = IDLE;
		System.out.println(message);
	}

	// ------------------------------------------------------------------
	// Teclado
	// ------------------------------------------------------------------

	public void startListeningNetwork() {
		transactionLayer.startListeningNetwork();
	}

	public void startListeningKeyboard() {
		try (Scanner scanner = new Scanner(System.in)) {
			while (true) {
				prompt();
				String line = scanner.nextLine();
				if (!line.isEmpty()) {
					command(line);
				}
			}
		} catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
		}
	}

	private void prompt() {
		System.out.println("");
		switch (state) {
		case IDLE:
			promptIdle();
			break;
		case CALLING:
			System.out.println("Llamando a " + peerName + "... (esperando respuesta)");
			break;
		case RINGING:
			System.out.println("Llamada entrante de " + peerName + ": S / N");
			break;
		case ANSWERED:
			System.out.println("Llamada contestada (esperando ACK)");
			break;
		case IN_CALL:
			System.out.println("En llamada como " + (isCaller ? "llamante" : "llamado") + ": BYE");
			break;
		case BYE_SENT:
			System.out.println("BYE enviado (esperando 200 OK)");
			break;
		default:
			throw new IllegalStateException("Unexpected state: " + state);
		}
		System.out.print("> ");
	}

	private void promptIdle() {
		System.out.println("INVITE xxx");
	}

	private void command(String line) throws IOException {
		String[] parts = line.trim().split("\\s+");
		// Mismo cerrojo que la capa de transaccion: el teclado y la red no se solapan
		synchronized (transactionLayer) {
			if (state == RINGING && parts.length == 1
					&& (parts[0].equalsIgnoreCase("S") || parts[0].equalsIgnoreCase("N"))) {
				answerCall(parts[0].equalsIgnoreCase("S"));
			} else if (parts[0].equalsIgnoreCase("INVITE")) {
				commandInvite(parts);
			} else if (parts[0].equalsIgnoreCase("BYE")) {
				commandBye();
			} else {
				System.out.println("Bad command");
			}
		}
	}

	private void commandInvite(String[] parts) throws IOException {
		if (state != IDLE) {
			System.out.println("Error: solo se soporta una llamada simultaneamente");
			return;
		}
		if (parts.length != 2) {
			System.out.println("Uso: INVITE usuarioDestino");
			return;
		}

		System.out.println("Inviting...");

		isCaller = true;
		callId = UUID.randomUUID().toString();
		callCSeq = 1;
		peerName = parts[1];
		peerUri = "sip:" + parts[1] + "@" + domain;

		SDPMessage sdpMessage = new SDPMessage();
		sdpMessage.setIp(this.myAddress);
		sdpMessage.setPort(this.rtpPort);
		sdpMessage.setOptions(RTPFLOWS);

		InviteMessage inviteMessage = new InviteMessage();
		inviteMessage.setDestination(peerUri);
		inviteMessage.setVias(new ArrayList<String>(Arrays.asList(this.myAddress + ":" + this.listenPort)));
		inviteMessage.setMaxForwards(70);
		inviteMessage.setToName(peerName);
		inviteMessage.setToUri(peerUri);
		inviteMessage.setFromName(userName);
		inviteMessage.setFromUri("sip:" + userName + "@" + domain);
		inviteMessage.setCallId(callId);
		inviteMessage.setcSeqNumber(String.valueOf(callCSeq));
		inviteMessage.setcSeqStr("INVITE");
		inviteMessage.setContact(myAddress + ":" + listenPort);
		inviteMessage.setContentType("application/sdp");
		inviteMessage.setContentLength(sdpMessage.toStringMessage().getBytes().length);
		inviteMessage.setSdp(sdpMessage);

		state = CALLING;
		transactionLayer.call(inviteMessage);
	}

	// ------------------------------------------------------------------
	// vitext (el fallo al lanzarlo no debe romper la senalizacion SIP)
	// ------------------------------------------------------------------

	private void runVitextClient() {
		try {
			vitextClient = Runtime.getRuntime().exec("xterm -e vitext/vitextclient -p 5000 239.1.2.3");
		} catch (IOException e) {
			System.err.println("No se ha podido lanzar vitextclient: " + e.getMessage());
		}
	}

	private void stopVitextClient() {
		if (vitextClient != null) {
			vitextClient.destroy();
			vitextClient = null;
		}
	}

	private void runVitextServer() {
		try {
			vitextServer = Runtime.getRuntime()
					.exec("xterm -iconic -e vitext/vitextserver -r 10 -p 5000 vitext/1.vtx 239.1.2.3");
		} catch (IOException e) {
			System.err.println("No se ha podido lanzar vitextserver: " + e.getMessage());
		}
	}

	private void stopVitextServer() {
		if (vitextServer != null) {
			vitextServer.destroy();
			vitextServer = null;
		}
	}

}
