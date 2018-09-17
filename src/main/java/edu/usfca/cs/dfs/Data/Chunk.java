package edu.usfca.cs.dfs.Data;



public class Chunk {
    byte[] data_chunk;
    String file_name = "";
    int chunk_id;
    boolean isLast;

    public Chunk(byte[] chunk, String file_name, int chunk_id)
    {
        this.data_chunk = chunk;
        this.file_name = file_name;
        this.chunk_id = chunk_id;
        isLast = false;
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

    public byte[] getData_chunk() {
        return data_chunk;
    }

    public boolean getIs_last(){
        return isLast;
    }

    public void set_last(){
        isLast = true;
    }
}
