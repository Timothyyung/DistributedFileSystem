package edu.usfca.cs.dfs.DataSender;

import edu.usfca.cs.dfs.StorageMessages;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

public class DataRequesterWithAck extends Thread{
    private StorageMessages.DataPacket dataPacket;
    private boolean sent;
    private String ipaddress;
    private int port;
    private static final Logger logger = LogManager.getRootLogger();
    public DataRequesterWithAck(StorageMessages.DataPacket dataPacket, String ipaddress, int port){
        this.dataPacket = dataPacket;
        this.sent = false;
        this.ipaddress = ipaddress;
        this.port = port;
    }

    @Override
    public void run() {
        logger.debug("Packet with ack sent to:  " + ipaddress +":"+Integer.toString(port));
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
                    logger.debug("Ack Recieved");
                    sent = true;
                }

            }catch (IOException | InterruptedException ie){
                ie.getStackTrace();
            }
        }
    }
}