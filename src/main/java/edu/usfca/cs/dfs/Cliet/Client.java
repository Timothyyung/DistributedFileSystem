package edu.usfca.cs.dfs.Cliet;

import com.google.protobuf.ByteString;
import edu.usfca.cs.dfs.Data.Chunk;
import edu.usfca.cs.dfs.Data.Data;
import edu.usfca.cs.dfs.StorageMessages;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

public class Client {

    public static void main(String[] args) {
        Data data = new Data("inputs/Mytestdoc.txt");
        System.out.println(data.getData().length);
        Client client = new Client();
        client.shard("localhost", 5050,data);
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        client.request_file_chunks("localhost", 5050, "a.txt");
    }

    public void shard(String ipaddress, int port, Data data)
    {
        int chunk_size = 10000;
        byte[] data_chunk = new byte[chunk_size];
        int i = 0; int j = 1; int k = 0;
        int b_val = data.getData().length;
        while(i < data.getData().length)
        {
            if(i % chunk_size == 0)
            {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                Chunk chunk = new Chunk(data_chunk,"a.txt",j);
                b_val -= chunk_size;
                data_chunk = new byte[check_size(chunk_size,b_val)];
                send(ipaddress,port,chunk);
                j += 1;
                k = 0;
            }
            data_chunk[k] = data.getData()[i];
            i += 1;
            k += 1;
        }
        System.out.println(data_chunk.length);
        Chunk chunk = new Chunk(data_chunk, "a.txt", j);
        send(ipaddress,port,chunk);


    }

    private int check_size(int chunk_size, int b_val)
    {
        if(b_val < 0)
            return chunk_size + b_val;
        else
            return chunk_size;
    }

    public void send(String ipaddress,int port, Chunk chunk){
        boolean sent = false;
        while(!sent) {
            try (
                    Socket s = new Socket(ipaddress, port);
                    OutputStream outputStream = s.getOutputStream();
            ) {
                ByteString bsval = ByteString.copyFrom(chunk.getData_chunk(), 0, chunk.getData_chunk().length);

                StorageMessages.Request s_chunk = StorageMessages.Request.newBuilder()
                        .setChunkId(chunk.getChunk_id())
                        .setData(bsval)
                        .setFileName(chunk.getFile_name())
                        .setOpcode(StorageMessages.Request.Op_code.store_chunk)
                        .build();
                StorageMessages.DataPacket dataPacket = StorageMessages.DataPacket.newBuilder()
                        .setRequest(s_chunk)
                        .build();

                dataPacket.writeDelimitedTo(outputStream);
                System.out.println("Chunk has been sent " + s_chunk.getChunkId());
                sent = true;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

   }

   public void request_file_chunks(String ipaddress,int port, String filename)
   {
       boolean sent = false;
       while(!sent){
           try(
                   Socket s = new Socket(ipaddress,port);
                   OutputStream outputStream = s.getOutputStream();
               ){
                    StorageMessages.Request s_chunk = StorageMessages.Request.newBuilder()
                            .setFileName(filename)
                            .setOpcode(StorageMessages.Request.Op_code.get_list)
                            .build();
                    s_chunk.writeDelimitedTo(outputStream);
                    sent = true;


           }catch (IOException ie){
               ie.getStackTrace();
           }
       }
   }
}