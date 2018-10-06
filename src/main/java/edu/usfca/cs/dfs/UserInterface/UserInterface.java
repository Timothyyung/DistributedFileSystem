package edu.usfca.cs.dfs.UserInterface;

import edu.usfca.cs.dfs.Cliet.Client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class UserInterface {
    private BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    public UserInterface(int port){
        System.out.println("Welcome to small data file transfer");
        Client client = new Client(port);
    }

    public void main_menu() throws IOException {
        System.out.println("Enter Command Here: For help type help?");
        boolean running = true;
        while(running){
            String cmd;
            cmd = br.readLine();
            switch (cmd){
                case "help":
                    help();
                    break;
                case "send_file":
                    send_file();
                    break;
            }
        }
    }

    private void help(){

    }

    private boolean send_file()
    {

    }

}
