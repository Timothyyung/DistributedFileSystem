package edu.usfca.cs.dfs.UserInterface;

import edu.usfca.cs.dfs.Cliet.Client;
import edu.usfca.cs.dfs.Coordinator.HashPackage.HashException;
import edu.usfca.cs.dfs.Data.Data;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class UserInterface {
    private BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    private Client client;
    private String coordip = "localhost";
    private int coordport = 6000;
    public UserInterface(int port){
        System.out.println("Welcome to small data file transfer");
        client = new Client(port);
    }

    public void main_menu() throws IOException, HashException {
        System.out.println("Enter Command Here: For help type help?");
        boolean running = true;
        while(running){
            String cmd;
            cmd = br.readLine();
            switch (cmd){
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
                case "request_max_chunks":
                    request_max();
                    break;
                case "request_chunk_map":
                    request_chunk_map();
                    break;

            }
        }
    }

    private void help(){

    }

    private void request_chunk_map() throws IOException {
        System.out.println("Ipaddress of storage node?");
        String ipaddress = br.readLine();
        System.out.println("Port of storage node");
        int port = Integer.parseInt(br.readLine());
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
        UserInterface userInterface = new UserInterface(7000);
        userInterface.main_menu();
    }

}


