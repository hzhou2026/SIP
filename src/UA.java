import ua.UaUserLayer;

public class UA {
	public static void main(String[] args) throws Exception {
		System.out.println("UA launching with args: " + String.join(", ", args));
		
		String userName = args[0];
		String domain = "SMA";
		if (userName.contains("@")) {
			domain = userName.substring(userName.indexOf('@') + 1);
			userName = userName.substring(0, userName.indexOf('@'));
		}
		int listenPort = Integer.parseInt(args[1]);
		String proxyAddress = args[2];
		int proxyPort = Integer.parseInt(args[3]);
		boolean debug = Boolean.parseBoolean(args[4]);
		int registerTime = Integer.parseInt(args[5]);

		UaUserLayer userLayer = new UaUserLayer(userName, domain, listenPort, proxyAddress, proxyPort, debug,
				registerTime);
		
		new Thread() {
			@Override
			public void run() {
				userLayer.startListeningNetwork();
			}
		}.start();
		
		// Registro en el proxy: bloquea hasta recibir el 200 OK (si es 404 el UA finaliza)
		userLayer.register();

		userLayer.startListeningKeyboard();
	}
}