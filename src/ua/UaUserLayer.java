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
import mensajesSIP.InviteMessage;
import mensajesSIP.RegisterMessage;
import mensajesSIP.SDPMessage;

public class UaUserLayer {
	private static final int IDLE = 0;
	private int state = IDLE;

	public static final ArrayList<Integer> RTPFLOWS = new ArrayList<Integer>(
			Arrays.asList(new Integer[] { 96, 97, 98 }));

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

	public UaUserLayer(String userName, String domain, int listenPort, String proxyAddress, int proxyPort,
			boolean debug, int registerTime) throws SocketException, UnknownHostException {
		this.userName = userName;
		this.domain = domain;
		this.registerTime = registerTime;
		this.transactionLayer = new UaTransactionLayer(listenPort, proxyAddress, proxyPort, debug, this);
		this.listenPort = listenPort;
		this.rtpPort = listenPort + 1;
	}

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

	public void onInviteReceived(InviteMessage inviteMessage) throws IOException {
		System.out.println("Received INVITE from " + inviteMessage.getFromName());
		runVitextServer();
	}

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
		default:
			throw new IllegalStateException("Unexpected state: " + state);
		}
		System.out.print("> ");
	}

	private void promptIdle() {
		System.out.println("INVITE xxx");
	}

	private void command(String line) throws IOException {
		if (line.startsWith("INVITE")) {
			commandInvite(line);
		} else {
			System.out.println("Bad command");
		}
	}

	private void commandInvite(String line) throws IOException {
		stopVitextServer();
		stopVitextClient();
		
		System.out.println("Inviting...");

		runVitextClient();

		String callId = UUID.randomUUID().toString();

		SDPMessage sdpMessage = new SDPMessage();
		sdpMessage.setIp(this.myAddress);
		sdpMessage.setPort(this.rtpPort);
		sdpMessage.setOptions(RTPFLOWS);

		InviteMessage inviteMessage = new InviteMessage();
		inviteMessage.setDestination("sip:bob@SMA");
		inviteMessage.setVias(new ArrayList<String>(Arrays.asList(this.myAddress + ":" + this.listenPort)));
		inviteMessage.setMaxForwards(70);
		inviteMessage.setToName("Bob");
		inviteMessage.setToUri("sip:bob@SMA");
		inviteMessage.setFromName("Alice");
		inviteMessage.setFromUri("sip:alice@SMA");
		inviteMessage.setCallId(callId);
		inviteMessage.setcSeqNumber("1");
		inviteMessage.setcSeqStr("INVITE");
		inviteMessage.setContact(myAddress + ":" + listenPort);
		inviteMessage.setContentType("application/sdp");
		inviteMessage.setContentLength(sdpMessage.toStringMessage().getBytes().length);
		inviteMessage.setSdp(sdpMessage);

		transactionLayer.call(inviteMessage);
	}

	private void runVitextClient() throws IOException {
		vitextClient = Runtime.getRuntime().exec("xterm -e vitext/vitextclient -p 5000 239.1.2.3");
	}

	private void stopVitextClient() {
		if (vitextClient != null) {
			vitextClient.destroy();
		}
	}

	private void runVitextServer() throws IOException {
		vitextServer = Runtime.getRuntime().exec("xterm -iconic -e vitext/vitextserver -r 10 -p 5000 vitext/1.vtx 239.1.2.3");
	}

	private void stopVitextServer() {
		if (vitextServer != null) {
			vitextServer.destroy();
		}
	}

}