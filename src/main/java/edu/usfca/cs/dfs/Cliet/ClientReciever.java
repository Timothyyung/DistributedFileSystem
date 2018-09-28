package edu.usfca.cs.dfs.Cliet;

import edu.usfca.cs.dfs.Data.Chunk;
import edu.usfca.cs.dfs.StorageMessages;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class ClientReciever extends Thread {
    private boolean running;
    private ConcurrentHashMap<String, Chunk> chunkmap;
    private String ipaddress;
    private int port;

    public ClientReciever(int port){
        this.running = true;
        try {
            ipaddress = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        chunkmap = new ConcurrentHashMap<>();
        this.port = port;
    }

    @Override
    public void run() {
        System.out.println("Very exited to recieve some Chunks");
        while (this.running){
            try(
                    ServerSocket serverSocket = new ServerSocket(this.port);
                    Socket sock = serverSocket.accept();
            ){
                System.out.println("data recieved " + Integer.toString(sock.getPort()));
                Client_reciever cr = new Client_reciever(sock);
                cr.start();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        System.out.println("The server is tired...");
    }


    private class Client_reciever extends Thread{
        private Socket s;
        public Client_reciever(Socket socket){
            this.s = socket;
        }

        @Override
        public synchronized void start() {
            try {
                InputStream instream = s.getInputStream();
                StorageMessages.DataPacket dataPacket = StorageMessages.DataPacket.parseDelimitedFrom(instream);
                s.close();
                System.out.println(dataPacket.toString());
                Chunk chunk = new Chunk(dataPacket);
                System.out.println("Recieving chunk" + Integer.toString(chunk.getChunk_id()));
                chunkmap.put(chunk.getFile_name() + Integer.toString(chunk.getChunk_id()),chunk);
                if(chunk.getIs_last()){
                    System.out.println(dataPacket.toString());
                    reassemble(dataPacket.getSinglechunk().getFileName(),dataPacket.getSinglechunk().getIpaddress(), dataPacket.getSinglechunk().getPort());
                }

            }catch(IOException e) {
                e.printStackTrace();
            }

        }

    }

    private void reassemble(String filename, String ipaddress, int port)
    {
        List<Byte> file = new ArrayList<>();
        boolean finished = false;
        int i = 1;
        byte[] chunk_bytes;
        while(!finished)
        {
            String key = filename + Integer.toString(i);
            if(chunkmap.containsKey(key))
                chunk_bytes = chunkmap.get(key).getData_chunk();
            else{
                chunk_bytes = request_chunk(filename, i, ipaddress,port);
                if(chunk_bytes == null)
                    return;
            }
            for(int j = 0; j < chunk_bytes.length ; j++)
                file.add(chunk_bytes[j]);

            i += 1;
            if(chunkmap.get(key).getIs_last()){
                finished = true;
            }
        }
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream("outputs");
            fos.write(toByteArray(file));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch ( IOException ie){
            ie.getStackTrace();
        }
        System.out.println("WOW WE DID IT");
    }

    private static byte[] toByteArray(List<Byte> in){
        final int n = in.size();
        byte ret[] = new byte[n];
        for (int i = 0; i < n ; i++){
            ret[i] = in.get(i);
        }
        return ret;
    }

    private byte[] request_chunk(String filename, int chunkid, String ipaddress, int port){
        int attempt = 10;
        while(attempt > 0) {
            try (
                    Socket s = new Socket(ipaddress, port);
                    InputStream inputStream = s.getInputStream();
                    OutputStream outputStream = s.getOutputStream();
            ) {
                StorageMessages.SingleChunk singleChunk = StorageMessages.SingleChunk.newBuilder()
                        .setFileName(filename)
                        .setChunkNumber(chunkid)
                        .setIpaddress(this.ipaddress)
                        .setPort(this.port)
                        .build();
                StorageMessages.DataPacket dataPacket = StorageMessages.DataPacket.newBuilder()
                        .setSinglechunk(singleChunk)
                        .build();
                dataPacket.writeDelimitedTo(outputStream);

                dataPacket = StorageMessages.DataPacket.parseDelimitedFrom(inputStream);
                Chunk chunk = new Chunk(dataPacket);
                return chunk.getData_chunk();
            } catch (IOException e) {
                e.printStackTrace();
            }
            attempt -= 1;
        }

        System.out.println("Chunk missing " + Integer.toString(chunkid));

        return null;
    }


}
