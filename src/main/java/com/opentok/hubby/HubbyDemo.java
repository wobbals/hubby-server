package com.opentok.hubby;

import java.util.ArrayList;

public class HubbyDemo {

    public static void main(String[] args) {
	int iterations = 16;
	if (args.length > 0) {
	    iterations = Integer.parseInt(args[0]);
	}
	try {
	    HubbyServer server = new HubbyServer(null, 26345);
	    ArrayList<HubbyClient> clients = new ArrayList<HubbyClient>();
	    for (int i=0; i < iterations; i++) {
		HubbyClient client = new HubbyClient("localhost", 26345, new Long(1), new Long(1));
		clients.add(client);
	    }
	    for (HubbyClient client : clients) {
		client.requestWrite(String.format("hi guys! I'm number %d",client.getClientID()).getBytes());
	    }
	    Thread.sleep(5000);
	    System.out.println("Should be quitting now.");
	    //server.stop();
	} catch (Exception e) {
	    e.printStackTrace();
	}

    }
}
