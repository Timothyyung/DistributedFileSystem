package edu.usfca.cs.dfs;

import com.google.protobuf.ByteString;
import edu.usfca.cs.dfs.Data.Chunk;
import edu.usfca.cs.dfs.Data.Data;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

public class Client {

    public static void main(String[] args) {

        byte[] a = new byte[100];
        Data data = new Data("inputs/Mytestdoc.txt");
        System.out.println(data.getData().length);
        Client client = new Client();
        client.shard(data);



    }

    public void shard(Data data){
        int b_len = 1000;
        byte[] a = new byte[b_len];
        Chunk chunk;


        int i = 0; int j = 1; int k = 0;
        int datalen = data.getData().length;
        while (i < data.getData().length) {
            if (i % b_len == 0) {
                chunk = new Chunk(a,"a.txt",j);
                send("localhost",5050,chunk);
                datalen = datalen - b_len;
                a = new byte[set_data_len(datalen,b_len)];
                j += 1;
                k = 0;
            }
            a[k] = data.getData()[i];
            i += 1;
        }
        chunk = new Chunk(a,"a.txt",j);
        send("localhost",5050,chunk);


        System.out.println("chunks sent " + j);
    }

    private int set_data_len(int datalen, int b_len)
    {
        if(datalen < 0)
            return datalen + b_len;
        else
            return b_len;
    }

    public void send(String ipaddress,int port, Chunk chunk){
       boolean sent = false;
       System.out.println("sending chunk " + chunk.getChunk_id());
       System.out.println("chunk size " + chunk.getData_chunk().length + "\n");
       while(!sent) {
            try (
                    Socket s = new Socket(ipaddress, port);
                    OutputStream outputStream = s.getOutputStream();
                    InputStream inputStream = s.getInputStream();
            ) {
                ByteString bsval = ByteString.copyFrom(chunk.getData_chunk(), 0, chunk.getData_chunk().length);
                StorageMessages.StoreChunk s_chunk = StorageMessages.StoreChunk.newBuilder()
                        .setChunkId(chunk.getChunk_id())
                        .setData(bsval)
                        .setFileName(chunk.getFile_name())
                        .build();
                s_chunk.writeDelimitedTo(outputStream);
                sent = true;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

   }


    public void request(String ipaddress,int port, String file_name){
        try(
                Socket s = new Socket(ipaddress,port);
                OutputStream outputStream = s.getOutputStream();
            ){
            StorageMessages.RetrieveFile r_file = StorageMessages.RetrieveFile.newBuilder()
                    .setFileName(file_name)
                    .build();
            r_file.writeDelimitedTo(outputStream);

        }catch(IOException e){
            e.printStackTrace();
        }
    }
}