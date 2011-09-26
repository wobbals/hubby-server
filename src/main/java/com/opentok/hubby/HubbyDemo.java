package com.opentok.hubby;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.Future;

public class HubbyDemo {

    public static void main(String[] args) {
	int iterations = 16;
	if (args.length > 0) {
	    iterations = Integer.parseInt(args[0]);
	}
	try {
	    HubbyServer server = new HubbyServer(null, 26345);

	    for (int i=0; i < iterations; i++) {
		final AsynchronousSocketChannel sc = AsynchronousSocketChannel.open();
		Future<Void> connected = sc.connect(new InetSocketAddress("localhost", 26345));
		connected.get();
		System.out.println("Connected!");
		final ByteBuffer buffer = ByteBuffer.allocate(512);
		//System.out.println(String.format("Read %d bytes" ,sc.read(buffer).get()));
		buffer.clear();
		buffer.putLong(i % 100); //divide users into 100 different buckets
		buffer.put(String.format("hey buddy! my name is %d", i).getBytes());
		buffer.flip();
		sc.write(buffer).get();
		buffer.clear();
		final int myId = i;
		sc.read(buffer, null, new CompletionHandler<Integer, Void>() {

		    public void completed(Integer result, Void attachment) {
			if (result > 0) {
			    System.out.println(String.format("%d received %d bytes of data.", myId, result));
			    byte[] data = new byte[result];
			    buffer.flip();
			    buffer.get(data);
			    System.out.println(String.format("data: %s", new String(data)));
			    buffer.clear();
			}
			sc.read(buffer, null, this);
		    }

		    public void failed(Throwable exc, Void attachment) {
			// TODO Auto-generated method stub

		    }});
		//Thread.sleep(100);

	    }
	    Thread.sleep(5000);
	    System.out.println("Should be quitting now.");
	    //server.stop();
	} catch (Exception e) {
	    e.printStackTrace();
	}

    }
}
