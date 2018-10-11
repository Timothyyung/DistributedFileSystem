package edu.usfca.cs.dfs.Main;

import edu.usfca.cs.dfs.UserInterface.UserInterface;

public class Client_Main {
    private int port;
    private int cport;
    private String cip;
    public Client_Main(String[] args)
    {
        for(int i = 0; i < args.length;i++){
            System.out.println(args[i]);
            if(args[i].equals("-port")){
                i++;
                this.port = Integer.parseInt(args[i]);
            }if(args[i].equals("-cip")){
                i++;
                this.cip = args[i];
            }if(args[i].equals("-cport")){
                i++;
                this.cport = Integer.parseInt(args[i]);
            }
        }
    }

    @Override
    public String toString() {
        return cip;
    }

    public static void main(String[] args) {

        Client_Main smain = new Client_Main(args);
        System.out.println(smain.cip);
        System.out.println(smain.cport);
        System.out.println(smain.port);
        UserInterface userInterface = new UserInterface(smain.port,smain.cip, smain.cport);

        userInterface.main_menu();


    }
}
