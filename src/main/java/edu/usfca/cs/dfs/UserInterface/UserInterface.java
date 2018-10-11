package edu.usfca.cs.dfs.UserInterface;

import edu.usfca.cs.dfs.Cliet.Client;
import edu.usfca.cs.dfs.Coordinator.HashPackage.HashException;
import edu.usfca.cs.dfs.Data.Data;

import java.awt.*;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class UserInterface {
    private BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    private Client client;
    private String coordip = "localhost";
    private int coordport = 6000;
    public UserInterface(int port, String coordip, int coordport){
        System.out.println("Welcome to small data file transfer");
        client = new Client(port);
        this.coordip = coordip;
        this.coordport = coordport;
    }

    public void main_menu(){
        System.out.println("Enter Command Here: For help type help?");
        boolean running = true;
        while(running){
            String cmd;
            System.out.println("Please enter a command: ");
            try {
                cmd = br.readLine();
                switch (cmd) {
                    case "help":
                        help();
                        break;
                    case "send_file_direct":
                        send_file_direct();
                        break;
                    case "send_file":
                        send_file();
                        break;
                    case "request_file":
                        request_file();
                        break;
                    case "request_map":
                        request_map();
                        break;
                    case "request_max":
                        request_max();
                        break;
                    case "request_chunk_map":
                        request_chunk_map();
                        break;
                    case "request_disk_space":
                        request_disk_space();
                        break;
                    case "request_total_disk_space":
                        request_t_disk_space();
                        break;
                    case "request_load":
                        request_requests_load();
                        break;

                }
            }catch(IOException ie){
                System.out.println("Something went wrong? Bad file name?");
            }catch(Exception e){
                System.out.println("Something ELSE went wrong... (but i don't know what)");
            }
        }
    }

    private void help(){
        System.out.println("_______");
        System.out.println("Send Files directly to nodes using hashmap retrieved from coordinator: send_file_direct ");
        System.out.println("Send Files to Storage nodes (Storage node will handle distribution):   send_file ");
        System.out.println("Request_file uses hashmap to retrieve from coordinator:                request_file");
        System.out.println("Request map from coordinator:                                          request_map");
        System.out.println("Request the number of chunks in a file                                 request_max");
        System.out.println("Request a list of chunks that a storage node has                       request_chunk_map");
        System.out.println("Request the available disk space                                       request_disk_space");
        System.out.println("Request request load from storage node                                 request_load");
        System.out.println("Request total disk space                                               request_total_disk_space");
        System.out.println("_______");
    }

    private void request_disk_space() throws IOException {
        System.out.println("Ipaddress of storage node?");
        String ipaddress = br.readLine();
        System.out.println("Port of storage node");
        int port = 0;
        try {
            port = Integer.parseInt(br.readLine());
        }catch(NumberFormatException ne){
            System.out.println("You need to put a number down");
        }
        client.request_disk_space(ipaddress,port);
    }

    private void request_requests_load() throws IOException{
        System.out.println("Ipaddress of storage node?");
        String ipaddress = br.readLine();
        System.out.println("Port of storage node");
        int port = 0;
        try {
            port = Integer.parseInt(br.readLine());
        }catch(NumberFormatException ne){
            System.out.println("You need to put a number down");
        }
        client.request_handled(ipaddress,port);
    }


    private void request_chunk_map() throws IOException {
        System.out.println("Ipaddress of storage node?");
        String ipaddress = br.readLine();
        System.out.println("Port of storage node");
        int port = 0;
        try {
            port = Integer.parseInt(br.readLine());
        }catch(NumberFormatException ne){
            System.out.println("You need to put a number down");
        }
        System.out.println("Requesting chunk map from " + ipaddress + ":" + Integer.toString(port));
        client.request_chunk_map(ipaddress,port);
    }

    private void request_max() throws IOException, HashException {
        System.out.println("Requesting max chunks: Please specify file");
        String filename = br.readLine();
        int max = client.find_max_chunks(filename);
        System.out.println(filename +": has " + Integer.toString(max) + " chunks");
    }

    private boolean send_file() throws IOException, HashException{
        System.out.println("Please type the path the to file you want to store");
        String path = br.readLine();
        System.out.println("Specify file name");
        String filename = br.readLine();
        System.out.println("What is the ip address of the storage node?");
        String ipaddress = br.readLine();
        System.out.println("What is the port of the storage node");
        int port = Integer.parseInt(br.readLine());
        Data data = new Data(path,filename);
        client.request_hashring(coordip , coordport);
        client.shard(ipaddress,port,data);
        return true;
    }

    private boolean send_file_direct() throws IOException, HashException {
        System.out.println("Please type the path the to file you want to store");
        String path = br.readLine();
        System.out.println("Specify file name");
        String filename = br.readLine();
        Data data = new Data(path,filename);
        client.request_hashring(coordip , coordport);
        client.run(data);
        return true;
    }

    private void request_file() throws IOException {
        System.out.println("Type the file name you want to retrieve");
        String filename = br.readLine();
        client.request_hashring(coordip, coordport);
        client.request_data_mkII(filename);
    }

    private void request_map() {
        System.out.println("requesting map....");
        client.request_hashring(coordip, coordport);
        System.out.println(client.hashring_toString());
    }

    public static void main(String[] args) throws IOException, HashException {
        UserInterface userInterface = new UserInterface(6000,"localhost", 7000);
        userInterface.main_menu();
    }

    private void request_t_disk_space(){
        System.out.println("Total Disk Space in cluster is:  " + Double.toString(client.request_total_disk_space(coordip, coordport)));
    }

}


