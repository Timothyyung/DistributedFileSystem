package edu.usfca.cs.dfs;

import edu.usfca.cs.dfs.Data.Chunk;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;


public class StorageNode extends Thread{

    private HashMap<String,Chunk> chunk_storage = new HashMap<>();
    private int hashspace;
    private ReentrantLock lock = new ReentrantLock();

    public static void main(String[] args) 
    throws Exception {
        String hostname = getHostname();
        System.out.println("Starting storage node on " + hostname + "...");
    }

    /**
     * Retrieves the short host name of the current host.
     *
     * @return name of the current host
     */
    private static String getHostname()
    throws UnknownHostException {
        return InetAddress.getLocalHost().getHostName();
    }


    public synchronized void store_chunk(Chunk chunk)
    {
        boolean isLocked = lock.tryLock();
        while(!isLocked)
        {
            try{
                isLocked = lock.tryLock(1, TimeUnit.SECONDS);
            }catch (InterruptedException e)
            {
                e.printStackTrace();
            }
        }
        try{
            chunk_storage.put(chunk.get_hash_key(),chunk);
        }finally {
            lock.unlock();
        }
    }

    public Chunk get_chunk(String key){
        return chunk_storage.get(key);
    }
    @Override
    public void run(){

    }

    public void startNode()
    {
        boolean run = true;
        System.out.println("Server Started");
        while (run){
            try(
                ServerSocket serverSocket = new ServerSocket(5050);
                Socket sock = serverSocket.accept();
            ){
                InputStream instream = sock.getInputStream();
                OutputStream outputStream = sock.getOutputStream();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }



}
