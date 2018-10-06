package edu.usfca.cs.dfs.DataSender;

import edu.usfca.cs.dfs.StorageMessages;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

public class DataRequesterWithAck extends Thread{
    private StorageMessages.DataPacket dataPacket;
    private boolean sent;
    private String ipaddress;
    private int port;

    public DataRequesterWithAck(StorageMessages.DataPacket dataPacket, String ipaddress, int port){
        this.dataPacket = dataPacket;
        this.sent = false;
        this.ipaddress = ipaddress;
        this.port = port;
    }

    @Override
    public void run() {
        System.out.println("Packet with ack sent to:  " + ipaddress +":"+Integer.toString(port));
        while(!sent){
            try(
                    Socket s = new Socket(this.ipaddress,this.port);
                    OutputStream outputStream = s.getOutputStream();
                    InputStream inputStream = s.getInputStream();
            ){

                dataPacket.writeDelimitedTo(outputStream);
                Thread.sleep(100);
                dataPacket = dataPacket.parseDelimitedFrom(inputStream);
               if(dataPacket.hasAck()) {
                    System.out.println("Ack Recieved");
                    sent = true;
                }

            }catch (IOException | InterruptedException ie){
                ie.getStackTrace();
            }
        }
        System.out.println("done");
    }
}