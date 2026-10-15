package proxy;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;

import mensajesSIP.SIPMessage;

public class ProxyTransportLayer {
	private static final int BUFSIZE = 4 * 1024;

	private int listenPort;
	private boolean debug;
	private DatagramSocket socket;
	private ProxyTransactionLayer transactionLayer;

	public ProxyTransportLayer(int listenPort, boolean debug, ProxyTransactionLayer transactionLayer)
			throws SocketException {
		this.transactionLayer = transactionLayer;
		this.listenPort = listenPort;
		this.debug = debug;
		this.socket = new DatagramSocket(listenPort);
	}

	public void send(SIPMessage sipMessage, String address, int port) throws IOException {
		String msg = sipMessage.toStringMessage();
		log("-->", msg);
		send(msg.getBytes(), address, port);
	}

	// Con debug se muestra el mensaje completo; sin debug, solo la primera linea.
	private void log(String arrow, String msg) {
		String text = msg.trim();
		if (!debug && text.contains("\n")) {
			text = text.substring(0, text.indexOf('\n')).trim();
		}
		System.out.println(arrow + " " + text);
		if (debug) {
			System.out.println();
		}
	}

	private void send(byte[] bytes, String address, int port) throws IOException {
		InetAddress inetAddress = InetAddress.getByName(address);
		DatagramPacket packet = new DatagramPacket(bytes, bytes.length, inetAddress, port);
		socket.send(packet);
	}

	public void startListening() {
		System.out.println("Listening at " + listenPort + "...");
		while (true) {
			try {
				byte[] buf = new byte[BUFSIZE];
				DatagramPacket packet = new DatagramPacket(buf, buf.length);
				socket.receive(packet);
				String msg = new String(packet.getData(), 0, packet.getLength());
				log("<--", msg);
				SIPMessage sipMessage = SIPMessage.parseMessage(msg);
				transactionLayer.onMessageReceived(sipMessage);
			} catch (Exception e) {
				System.err.println(e.getMessage());
				e.printStackTrace();
			}
		}
	}

}