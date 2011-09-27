package com.opentok.hubby;

import java.util.ArrayList;

public class HubbyDemo {

    public static void main(String[] args) {
	String mode = args[0];
	if (mode == null || mode.isEmpty()) {
	    System.out.println("Usage: HubbyDemo mode [host port iterations]");
	    System.exit(0);
	}
	boolean doServer = false;
	boolean doClient = false;
	int iterations = 16;
	int bindPort = 26354;
	String host = "localhost";
	if (mode.equalsIgnoreCase("server")) {
	    doServer = true;
	    bindPort = Integer.parseInt(args[1]);
	} else if (mode.equalsIgnoreCase("client")) {
	    host = args[1];
	    bindPort = Integer.parseInt(args[2]);
	    iterations = Integer.parseInt(args[3]);
	    doClient = true;
	} else {
	    doClient = true;
	    doServer = true;
	}
	 
	try {
	    if (doServer) {
		System.out.println("Doing server");
	    	HubbyServer server = new HubbyServer(null, bindPort);
	    }
	    if (doClient) {
		System.out.println("Doing client");
		ArrayList<HubbyClient> clients = new ArrayList<HubbyClient>();
		for (int i=0; i < iterations; i++) {
		    HubbyClient client = new HubbyClient(host, bindPort, new Long(1), new Long(i));
		    clients.add(client);
		}
		for (HubbyClient client : clients) {
		    client.requestWrite(String.format("<connection id=%d />",client.getClientID()).getBytes());
		}
	    }
	    while (true) {
		Thread.sleep(1000);
	    }
	} catch (Exception e) {
	    e.printStackTrace();
	}

    }
}
