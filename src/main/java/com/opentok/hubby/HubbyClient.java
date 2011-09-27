package com.opentok.hubby;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.WritePendingException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class HubbyClient {
    private AsynchronousSocketChannel channel;
    private IncomingData incomingDataHandler;
    private ByteBuffer inBuffer;
    private ByteBuffer outBuffer;
    private ConcurrentLinkedQueue<Runnable> writeQueue;
    private OutgoingData outgoingDataHandler;
    private Long hubID;
    private Long clientID;
    
    private static AsynchronousChannelGroup channelGroup;
    
    static {
	try {
	    channelGroup = AsynchronousChannelGroup.withThreadPool(Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()));
	} catch (IOException e) {
	    e.printStackTrace();
	}
    }

    
    public HubbyClient(String host, int port, Long hubID, Long clientID) throws InterruptedException, ExecutionException, IOException {
	inBuffer = ByteBuffer.allocate(4096);
	outBuffer = ByteBuffer.allocate(4096);
	incomingDataHandler = new IncomingData();
	outgoingDataHandler = new OutgoingData();
	writeQueue = new ConcurrentLinkedQueue<Runnable>();
	this.hubID = hubID;
	this.clientID = clientID;
	
	channel = AsynchronousSocketChannel.open(channelGroup);
	Future<Void> connected = channel.connect(new InetSocketAddress(host, 26345));
	connected.get();
	inBuffer.putLong(hubID);
	inBuffer.flip();
	channel.write(inBuffer).get();
	System.out.println(String.format("[client] %d connected", clientID));
	channel.read(inBuffer, null, incomingDataHandler);
    }

    private class IncomingData implements CompletionHandler<Integer, Void> {

	public void completed(Integer result, Void attachment) {
	    if (result > 0) {
		System.out.println(String.format("[client] %d received %d bytes of data.", clientID, result));
		byte[] data = new byte[result];
		inBuffer.flip();
		inBuffer.get(data);
		System.out.println(String.format("[client] data: %s", new String(data)));
	    }
	    inBuffer.clear();
	    channel.read(inBuffer, null, this);
	}

	public void failed(Throwable exc, Void attachment) {
	    exc.printStackTrace();
	}

    }
    
    
    private class OutgoingData implements CompletionHandler<Integer, Void> {

	public void completed(Integer result, Void attachment) {
	    System.out.println(String.format("[client] %d sent %d bytes of data", clientID, result));
	    if (!writeQueue.isEmpty()) {
		System.out.println("Continuing down write queue");
		writeQueue.poll().run();
	    }
	}

	public void failed(Throwable exc, Void attachment) {
	    // TODO Auto-generated method stub
	    exc.printStackTrace();
	}

    }
    
    private class WriteTask implements Runnable {
	private ByteBuffer data;

	public WriteTask(ByteBuffer data) {
	    this.data = data;
	}

	public void run() {
	    channel.write(data, null, outgoingDataHandler);
	}
    }
    
    public void requestWrite(byte[] data) {
	ByteBuffer buffer = ByteBuffer.allocate(data.length);
	buffer.put(data);
	buffer.flip();
	try {
	    channel.write(buffer, null, outgoingDataHandler);
	    System.out.println("[client] Write submitted to channel");
	} catch (WritePendingException e) {
	    System.out.println("[client] Queueing incoming write");
	    buffer.rewind();
	    writeQueue.offer(new WriteTask(buffer));
	}
	
    }

    public Long getClientID() {
	return this.clientID;
    }
}
