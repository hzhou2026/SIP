import proxy.ProxyUserLayer;

public class Proxy {
	public static void main(String[] args) throws Exception {
		System.out.println("Proxy launching with args: " + String.join(", ", args));
		int listenPort = 5060;
		boolean looseRouting = true;
		boolean debug = true;
		if (args.length > 0) {
			listenPort = Integer.parseInt(args[0]);
			looseRouting = Boolean.parseBoolean(args[1]);
			debug = Boolean.parseBoolean(args[2]);
		}
		System.out.println("Debug Mode: " + debug);
		System.out.println("Loose Routing: " + looseRouting);
		ProxyUserLayer userLayer = new ProxyUserLayer(listenPort, debug);
		userLayer.startListening();
	}
}