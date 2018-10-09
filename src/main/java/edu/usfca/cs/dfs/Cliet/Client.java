package edu.usfca.cs.dfs.Cliet;

import com.google.protobuf.ByteString;
import edu.usfca.cs.dfs.CoordMessages;
import edu.usfca.cs.dfs.Coordinator.HashPackage.HashException;
import edu.usfca.cs.dfs.Coordinator.HashPackage.HashRingEntry;
import edu.usfca.cs.dfs.Coordinator.HashPackage.SHA1;
import edu.usfca.cs.dfs.Coordinator.HashRing;
import edu.usfca.cs.dfs.Data.Chunk;
import edu.usfca.cs.dfs.Data.Data;
import edu.usfca.cs.dfs.DataSender.DataRequester;
import edu.usfca.cs.dfs.DataSender.DataRequesterRunnable;
import edu.usfca.cs.dfs.DataSender.DataRequesterWithAck;
import edu.usfca.cs.dfs.Storage.StorageNode;
import edu.usfca.cs.dfs.StorageMessages;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Client extends Thread{

    private String ipaddress;
    private int port;
    private HashRing<byte[]> hashRing;
    private SHA1 sha1 = new SHA1();
    private ClientReciever clientReciever;


    /*
    Client class will be used to interact with storage nodes...
    Will be able to
    - Shard data
    - Get data from nodes
    - View chunk list from a specfic storage node
    - Get the number of chunks in a file

     */

    public Client(int port){
        try {
            ipaddress = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        this.port = port;
        this.hashRing = new HashRing<>(sha1);
        clientReciever = new ClientReciever(port);
        clientReciever.start();
    }

/*
Sharding allows for the user to input a file ( right now can only handle TXT files ) and split them up into
different chunks to be sent to the storage nodes. We will allow the storage nodes to handle all storage operations
 */


    public void run(Data data) {
        try {
            shard(data);
        } catch (HashException e) {
            e.printStackTrace();
        }
    }

    public void shard(Data data) throws HashException {
        int chunk_size = 10000;
        byte[] data_chunk = new byte[chunk_size];
        int i = 0; int j = 1; int k = 0;
        int b_val = data.getData().length - chunk_size;
        while(i < data.getData().length)
        {
            if(i % chunk_size == 0 & i != 0)
            {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                Chunk chunk = new Chunk(data_chunk,data.getFilename(),j,false);
                b_val = b_val - chunk_size;
                System.out.println( b_val);
                send_chunk_direct(chunk,data_chunk);
                data_chunk = new byte[check_size(chunk_size,b_val)];

                j += 1;
                k = 0;
            }
            data_chunk[k] = data.getData()[i];
            i += 1;
            k += 1;
        }
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println(data_chunk.length);
        Chunk chunk = new Chunk(data_chunk, data.getFilename(), j,true);
        send_chunk_direct(chunk,data_chunk);
    }


    public void shard(String ipaddress, int port, Data data) throws HashException {
        int chunk_size = 10000;
        byte[] data_chunk = new byte[chunk_size];
        int i = 0; int j = 1; int k = 0;
        int b_val = data.getData().length - chunk_size;
        while(i < data.getData().length)
        {
            if(i % chunk_size == 0 & i != 0)
            {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                Chunk chunk = new Chunk(data_chunk,data.getFilename(),j,false);
                b_val = b_val - chunk_size;
                System.out.println( b_val);
                send_chunk(ipaddress,port,chunk, data_chunk);
                data_chunk = new byte[check_size(chunk_size,b_val)];

                j += 1;
                k = 0;
            }
            data_chunk[k] = data.getData()[i];
            i += 1;
            k += 1;
        }
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println(data_chunk.length);
        Chunk chunk = new Chunk(data_chunk, data.getFilename(), j,true);
        send_chunk(ipaddress,port,chunk,data_chunk);
    }

    private int check_size(int chunk_size, int b_val)
    {
        if(b_val < 0)
            return chunk_size + b_val;
        else
            return chunk_size;
    }

    public void send_chunk(String ipaddress, int port, Chunk chunk, byte[] datachunk){
        ByteString bsval = ByteString.copyFrom(datachunk, 0, datachunk.length);
        StorageMessages.Request s_chunk = StorageMessages.Request.newBuilder()
                .setChunkId(chunk.getChunk_id())
                .setData(bsval)
                .setFileName(chunk.getFile_name())
                .setOpcode(StorageMessages.Request.Op_code.store_chunk)
                .setIslast(chunk.getIs_last())
                .setIpaddress(ipaddress)
                .setPort(port)
                .build();
        StorageMessages.DataPacket dataPacket = StorageMessages.DataPacket.newBuilder()
                .setRequest(s_chunk)
                .build();
        DataRequester requester = new DataRequester(dataPacket,ipaddress,port);
        requester.start();
        System.out.println("Chunk has been sent " + s_chunk.getChunkId() +s_chunk.getData());
    }



    public void send_chunk_direct(Chunk chunk, byte[] datachunk) throws HashException {
        ByteString bsval = ByteString.copyFrom(datachunk, 0, datachunk.length);
        StorageMessages.Request s_chunk = StorageMessages.Request.newBuilder()
                .setChunkId(chunk.getChunk_id())
                .setData(bsval)
                .setFileName(chunk.getFile_name())
                .setOpcode(StorageMessages.Request.Op_code.store_chunk)
                .setIslast(chunk.getIs_last())
                .build();
        StorageMessages.DataPacket dataPacket = StorageMessages.DataPacket.newBuilder()
                .setRequest(s_chunk)
                .build();
        String key = key_gen(chunk.getFile_name(),chunk.getChunk_id(),chunk.getIs_last());
        BigInteger pos = hashRing.locate((key.getBytes()));
        HashRingEntry node = hashRing.returnNode(pos);

        DataRequesterWithAck requester = new DataRequesterWithAck(dataPacket,node.inetaddress,node.port);
        requester.start();

        System.out.println("Chunk has been sent " + s_chunk.getChunkId() +s_chunk.getData());
    }

   public void request_data_mkII(String filename)
   {
       try {
       int max_chunk = find_max_chunks(filename);
       boolean last  = false;
       for(int i = 1; i <= max_chunk; i++){
                if( i == max_chunk)
                    last = true;
               request_from_storage(filename,i,ipaddress,port,last);
               Thread.sleep(1000);
           }
           System.out.println(max_chunk);

       } catch (HashException | InterruptedException e) {
           e.printStackTrace();
       }
   }

   public int find_max_chunks(String filename) throws HashException {
       BigInteger pos = hashRing.locate((filename + "last").getBytes());
       HashRingEntry node = hashRing.returnNode(pos);
        try(
               Socket s = new Socket(node.inetaddress,node.port);
               OutputStream outputStream = s.getOutputStream();
               InputStream inputStream = s.getInputStream()
        ) {

           StorageMessages.SingleChunk singleChunk = StorageMessages.SingleChunk.newBuilder()
                   .setFileName(filename)
                   .setIsLast(true)
                   .build();
           StorageMessages.DataPacket dataPacket = StorageMessages.DataPacket.newBuilder()
                   .setSinglechunk(singleChunk)
                   .build();

           dataPacket.writeDelimitedTo(outputStream);

           dataPacket = StorageMessages.DataPacket.parseDelimitedFrom(inputStream);
           return dataPacket.getSinglechunk().getChunkNumber();
       }catch(IOException ioe){
           ioe.printStackTrace();
       }
       return -1;
   }

   private void request_from_storage(String filename, int chunknumber, String ipaddress, int port,boolean islast) throws HashException {
        StorageMessages.Request request = StorageMessages.Request.newBuilder()
                .setOpcode(StorageMessages.Request.Op_code.get_chunk)
                .setFileName(filename)
                .setChunkId(chunknumber)
                .setIpaddress(ipaddress)
                .setIslast(islast)
                .setPort(port)
                .build();
        StorageMessages.DataPacket dataPacket = StorageMessages.DataPacket.newBuilder()
                .setRequest(request)
                .build();
        String key = key_gen(filename,chunknumber,islast);
        BigInteger pos = hashRing.locate((key.getBytes()));
        HashRingEntry node = hashRing.returnNode(pos);
        DataRequester dataRequester = new DataRequester(dataPacket,node.inetaddress ,node.port);
        dataRequester.start();
    }



    private String key_gen(String filename, int chunknumber, boolean islast){
        if(islast)
            return filename + "last";
        else
            return filename + Integer.toString(chunknumber);
    }


    public void request_hashring(String ipaddress,int port)
   {
       boolean sent = false;
       while(!sent){
           try(
                   Socket s = new Socket(ipaddress,port);
                   OutputStream outputStream = s.getOutputStream();
                   InputStream inputStream = s.getInputStream()
           ){
               CoordMessages.RequestMap request = CoordMessages.RequestMap.newBuilder()
                       .setIpaddress(this.ipaddress)
                       .setPort(this.port)
                       .build();
               CoordMessages.DataPacket dataPacket = CoordMessages.DataPacket.newBuilder()
                       .setRequestmap(request)
                       .build();
               dataPacket.writeDelimitedTo(outputStream);

               CoordMessages.DataPacket response;
               response = CoordMessages.DataPacket.parseDelimitedFrom(inputStream);

               hashRing = new HashRing(sha1,response.getHashring());


               sent = true;
           }catch (IOException ie){
               ie.getStackTrace();
           }
       }
   }

    private int make_check_sum(byte[] data_chunk)
    {
        int sum = 0;
        for (int i = 0; i < data_chunk.length; i++){
            sum += data_chunk[i];
        }
        return sum;
    }

    public String hashring_toString()
    {
        return hashRing.toString();
    }

    public static void main(String[] args) throws HashException {

        Data data = new Data("inputs/dennis.txt","a.txt");
        System.out.println(data.getData().length);
        Client client = new Client(7000);
        client.request_hashring("localhost",6000);
        client.shard(data);
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println(client.find_max_chunks("a.txt"));
        client.request_data_mkII("a.txt");
    }

}