package edu.usfca.cs.dfs.DataSender;

import edu.usfca.cs.dfs.StorageMessages;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

public class DataRequesterRunnable implements Runnable{
    private StorageMessages.DataPacket dataPacket;
    private boolean sent;
    private String ipaddress;
    private int port;

    public DataRequesterRunnable(StorageMessages.DataPacket dataPacket, String ipaddress, int port){
        this.dataPacket = dataPacket;
        this.sent = false;
        this.ipaddress = ipaddress;
        this.port = port;
    }

    @Override
    public synchronized void run() {
        System.out.println("Packet sent to:  " + ipaddress +":"+Integer.toString(port));
        System.out.println(sent);
        System.out.println(dataPacket.getRequest().getFileName());
        int trails = 0;



        try {
            Socket s = new Socket(this.ipaddress,this.port);
            OutputStream outputStream = s.getOutputStream();
            InputStream inputStream = s.getInputStream();

            dataPacket.writeDelimitedTo(outputStream);
            /*dataPacket = dataPacket.parseDelimitedFrom(inputStream);
           if(dataPacket.hasAck()) {
                System.out.println("Ack Recieved");
                sent = true;
            }
            else
                trails += 1;
                */
            sent = true;

        } catch (IOException e) {
            e.printStackTrace();
        }




        System.out.println("done");
    }
}