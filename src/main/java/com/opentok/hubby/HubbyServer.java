package com.opentok.hubby;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.InterruptedByTimeoutException;
import java.nio.channels.WritePendingException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Binds to a local port, accepts connections and groups them by the opening preamble.
 * Future incoming data is copied to any connected socket with the same preamble.
 * @author charley
 *
 */
public class HubbyServer {
    private AsynchronousServerSocketChannel ssc;
    private AtomicLong connectionSerial;
    private ConcurrentHashMap<Long, BufferPair> buffers;
    private ConcurrentHashMap<Long, AsynchronousSocketChannel> channels;
    private ConcurrentHashMap<Long, ConcurrentLinkedQueue<Long>> hubs;
    private ConcurrentHashMap<Long, ConcurrentLinkedQueue<Runnable>> writeTasks;
    private ConcurrentHashMap<Long, Long> hubMemberships;
    private IncomingData initialIncomingDataHandler;
    private OutgoingData outgoingDataHandler;
    
    public HubbyServer(InetAddress host, int port) throws IOException {
	connectionSerial = new AtomicLong();
	AsynchronousChannelGroup channelGroup = AsynchronousChannelGroup.withThreadPool(Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()));
	ssc = AsynchronousServerSocketChannel.open(channelGroup).bind(new InetSocketAddress(host, port));
	ssc.accept(connectionSerial.getAndIncrement(), new ServerSocketAccepted());
	buffers = new ConcurrentHashMap<Long, BufferPair>();
	channels = new ConcurrentHashMap<Long, AsynchronousSocketChannel>();
	writeTasks = new ConcurrentHashMap<Long, ConcurrentLinkedQueue<Runnable>>();
	hubs = new ConcurrentHashMap<Long, ConcurrentLinkedQueue<Long>>();
	hubMemberships = new ConcurrentHashMap<Long, Long>();
	initialIncomingDataHandler = new IncomingData();
	outgoingDataHandler = new OutgoingData();
    }
    
    private class BufferPair {
	public ByteBuffer in;
	public ByteBuffer out;
    }
    
    private class ServerSocketAccepted implements CompletionHandler<AsynchronousSocketChannel, Long> {

	public void completed(final AsynchronousSocketChannel result, Long attachment) {
	    ssc.accept(connectionSerial.getAndIncrement(), this);
	    System.out.println(String.format("[server] Socket connected. ID=%d",attachment));
	    channels.put(attachment, result);
	    BufferPair bufferPair = new BufferPair();
	    bufferPair.in = ByteBuffer.allocate(4096);
	    bufferPair.out = ByteBuffer.allocate(4096);
	    buffers.put(attachment, bufferPair);
	    bufferPair.in.clear();
	    result.read(bufferPair.in, attachment, initialIncomingDataHandler);
	}

	public void failed(Throwable exc, Long attachment) {
	    new Exception().printStackTrace();
	}
	
    }

    private class IncomingData implements CompletionHandler<Integer, Long> {

	public void completed(Integer result, Long attachment) {
	    ByteBuffer buffer = buffers.get(attachment).in;
	    buffer.flip();
	    AsynchronousSocketChannel channel = channels.get(attachment);

	    if (!hubMemberships.containsKey(attachment) && result >= (Long.SIZE / 8)) {
		//we have enough data to assign preamble
		Long preamble = buffer.getLong();
		System.out.println(String.format("[server] %d is now a member of %d", attachment, preamble));
		if (!hubs.containsKey(preamble)) {
		    ConcurrentLinkedQueue<Long> channelIds = new ConcurrentLinkedQueue<Long>();
		    hubs.put(preamble, channelIds);
		}
		hubs.get(preamble).offer(attachment);
		hubMemberships.put(attachment, preamble);
		result -= (Long.SIZE / 8);
	    }
	    
	    if (buffer.hasRemaining() && buffer.position() > 0) {
		//shift remaining data to front of buffer
		int remaining = buffer.remaining();
		int offset = buffer.position();
		for (int i=0; i < remaining; i++) {
		    buffer.put(i, buffer.get(i+offset));
		}
		buffer.limit(remaining);
		buffer.position(0);
		//send remaining to hub
	    }
	    
	    if (result > 0) {
		enqueueHubWrites(buffer, attachment);
		buffer.clear();
	    }

	    channel.read(buffer, attachment, this);
	}

	public void failed(Throwable exc, Long attachment) {
	    exc.printStackTrace();
	}
	
    }
    
    private class OutgoingData implements CompletionHandler<Integer, Long> {

	public void completed(Integer result, Long attachment) {
	    System.out.println(String.format("[server] %d successfully sent %d bytes", attachment, result));
	    ConcurrentLinkedQueue<Runnable> queue = writeTasks.get(attachment);
	    if (!queue.isEmpty()) {
		System.out.println("[server] Continuing down write queue");
		queue.poll().run();
	    }
	}

	public void failed(Throwable exc, Long attachment) {
	    //exc.printStackTrace();
	}
	
    }

    public synchronized void stop() {
	for (Long channelID : channels.keySet()) {
	    try {
		channels.get(channelID).close();
	    } catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	    }
	    channels.remove(channelID);
	}
	try {
	    ssc.close();
	} catch (IOException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
    }
    
    private class EnqueueWriteTask implements Runnable {
	private AsynchronousSocketChannel channel;
	private ByteBuffer buffer;
	private Long channelID;
	
	public EnqueueWriteTask(AsynchronousSocketChannel channel, Long channelID, ByteBuffer buffer) {
	    this.buffer = buffer;
	    this.channel = channel;
	    this.channelID = channelID;
	}
	
	public void run() {
	    channel.write(buffer, channelID, outgoingDataHandler);
	}
    }
    
    public void offerWriteTask(EnqueueWriteTask task, Long channelID) {
	ConcurrentLinkedQueue<Runnable> queue = writeTasks.get(channelID);
	if (queue == null) {
	    queue = new ConcurrentLinkedQueue<Runnable>();
	    writeTasks.put(channelID, queue);
	}
	try {
	    task.run();
	} catch (WritePendingException e) {	    
	    queue.offer(task);
	}
    }
    
    public void enqueueHubWrites(ByteBuffer buffer, Long fromChannelID) {
	Long hubID = hubMemberships.get(fromChannelID);
	int counter = 0;
	for (Long channelID : hubs.get(hubID)) {
	    if (channelID != fromChannelID) {
		counter++;
		AsynchronousSocketChannel outChannel = channels.get(channelID);
		ByteBuffer outBuffer = buffers.get(channelID).out;
		outBuffer.clear();
		buffer.rewind();
		outBuffer.put(buffer);
		outBuffer.flip();
		offerWriteTask(new EnqueueWriteTask(outChannel, channelID, outBuffer), channelID);
	    }
	}
	System.out.println(String.format("[server] Scheduled dissemination of %d bytes from connection %d to %d peers", buffer.limit(), fromChannelID, counter));
    }

}
