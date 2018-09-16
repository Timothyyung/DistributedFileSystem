package edu.usfca.cs.dfs.Data;



public class Chunk {
    byte[] chunk;
    String file_name = "";
    int chunk_id;

    public Chunk(byte[] chunk, String file_name, int chunk_id)
    {
        this.chunk = chunk;
        this.file_name = file_name;
        this.chunk_id = chunk_id;
    }

    public String get_hash_key(){
        return file_name+Integer.toString(chunk_id);
    }


}
