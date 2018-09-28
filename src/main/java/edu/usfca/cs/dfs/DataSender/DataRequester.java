package edu.usfca.cs.dfs.DataSender;

import edu.usfca.cs.dfs.StorageMessages;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

public class DataRequester extends Thread{
    private StorageMessages.DataPacket dataPacket;
    private boolean sent;
    private String ipaddress;
    private int port;

    public DataRequester(StorageMessages.DataPacket dataPacket, String ipaddress, int port){
        this.dataPacket = dataPacket;
        this.sent = false;
        this.ipaddress = ipaddress;
        this.port = port;
    }

    @Override
    public void run() {
        System.out.println("Packet sent to:  " + ipaddress +":"+Integer.toString(port));
        while(!sent){
            try(
                    Socket s = new Socket(this.ipaddress,this.port);
                    OutputStream outputStream = s.getOutputStream();
            ){
                dataPacket.writeDelimitedTo(outputStream);
                sent = true;
            }catch (IOException ie){
                ie.getStackTrace();
            }
        }
    }
}
