package edu.usfca.cs.dfs.Data;


import edu.usfca.cs.dfs.StorageMessages;

public class Chunk {
    //byte[] data_chunk;
    int checksum;
    String file_name = "";
    int chunk_id;
    boolean isLast;

    public Chunk(StorageMessages.DataPacket dataPacket){
        if(dataPacket.hasSinglechunk()){
          //  data_chunk = dataPacket.getSinglechunk().getData().toByteArray();
            file_name = dataPacket.getSinglechunk().getFileName();
            chunk_id = dataPacket.getSinglechunk().getChunkNumber();
            isLast = dataPacket.getSinglechunk().getIsLast();
            this.checksum = make_check_sum(dataPacket.getSinglechunk().getData().toByteArray());
        }else
            System.out.println("Wrong Packet");
    }

    public Chunk(byte[] chunk, String file_name, int chunk_id, boolean isLast)
    {
      //  this.data_chunk = chunk;
        this.file_name = file_name;
        this.chunk_id = chunk_id;
        this.isLast = isLast;
        this.checksum = make_check_sum(chunk);
    }

    private int make_check_sum(byte[] data_chunk)
    {
        int sum = 0;
        for (int i = 0; i < data_chunk.length; i++){
            sum += data_chunk[i];
        }
        return sum;
    }

    public int getChecksum() {
        return checksum;
    }

    public String get_hash_key(){
        return file_name+Integer.toString(chunk_id);
    }

    public int getChunk_id() {
        return chunk_id;
    }

    public String getFile_name() {
        return file_name;
    }

   // public byte[] getData_chunk() {return data_chunk;}

    public boolean getIs_last(){
        return isLast;
    }

}
