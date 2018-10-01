package edu.usfca.cs.dfs.Data;

import java.io.*;


public class Data {
    private byte[] data;
    private String filename;

    public Data(byte[] data, String filename){
        this.filename = filename;
        this.data = data;
    }

    public Data(String path, String filename){
        File file = new File(path);
        try {
            data = read(file);
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.filename = filename;
    }

    private byte[] read(File file) throws IOException {
        ByteArrayOutputStream out = null;
        InputStream in = null;
        try{
            byte[] buffer = new byte[4096];
            out = new ByteArrayOutputStream();
            in = new FileInputStream(file);
            int read = 0;
            while((read = in.read(buffer)) != 1){
                out.write(buffer,0,read);
            }
        }finally{
            try{
                if(out != null)
                    out.close();
            }catch(IOException e){
                e.getStackTrace();
            }

            try {
                if(in != null)
                    in.close();
            }catch(IOException e){
                e.getStackTrace();
            }
            return out.toByteArray();
        }
    }

    public byte[] getData() {
        return data;
    }
    public String getFilename(){return filename;}
}
