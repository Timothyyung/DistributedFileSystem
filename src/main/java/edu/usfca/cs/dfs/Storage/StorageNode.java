package edu.usfca.cs.dfs.Storage;

import com.google.protobuf.ByteString;
import edu.usfca.cs.dfs.CoordMessages;
import edu.usfca.cs.dfs.Coordinator.HashPackage.HashException;
import edu.usfca.cs.dfs.Coordinator.HashPackage.HashRingEntry;
import edu.usfca.cs.dfs.Coordinator.HashPackage.HashTopologyException;
import edu.usfca.cs.dfs.Coordinator.HashPackage.SHA1;
import edu.usfca.cs.dfs.Coordinator.HashRing;
import edu.usfca.cs.dfs.Data.Chunk;
import edu.usfca.cs.dfs.DataSender.DataRequester;
import edu.usfca.cs.dfs.DataSender.DataRequesterWithAck;
import edu.usfca.cs.dfs.StorageMessages;

import java.io.*;
import java.math.BigInteger;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;



public class StorageNode extends Thread{

    private ConcurrentHashMap<String,Chunk> chunk_storage = new ConcurrentHashMap<>();
    private HashRing<byte[]> hashRing;
    private SHA1 sha1 = new SHA1();
    private String ipaddress;
    private int port;
    private boolean run;
    private Heartbeat heartbeat;
    private File dir;
    private BigInteger mypos;
    private String path;
    private int number_request_handled;
    private static final Logger logger = LogManager.getRootLogger();

    public StorageNode(int port,String coordip,int coordport){
        try {
            ipaddress = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        this.port = port;
        this.number_request_handled = 0;
        path = "bigdata/"+ Integer.toString(port) +"/tyung";
        dir = new File(path);
        dir.mkdirs();
        this.run = request_access(coordip,coordport);
        if(this.run && hashRing.get_size() > 1)
        {
            get_chunks_from_neighbor();
        }
        heartbeat = new Heartbeat(hashRing.get_size(),ipaddress,port,coordip,coordport);
        heartbeat.start();
    }

    /**
     * Retrieves the short host name of the current host.
     *
     * @return name of the current host
     */
    private static String getHostname()
    throws UnknownHostException {
        return InetAddress.getLocalHost().getHostName();
    }

    @Override
    public String toString() {
        return ipaddress + Integer.toString(port);
    }

    @Override
    public void start(){
        startNode();
    }

    public void startNode()
    {
        System.out.println("Server Started");
        while (run){
            try(
                ServerSocket serverSocket = new ServerSocket(this.port);
            ){
                store_chunk_listener scl = new store_chunk_listener(serverSocket.accept());
                scl.start();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        System.out.println("Server Ending");
    }

    private void get_chunks_from_neighbor()
    {
        System.out.println("Getting chunks from neighbors");
        HashRingEntry hre =  hashRing.get_previous_entry(mypos);
        try(
                Socket s = new Socket(hre.inetaddress,hre.port);
                OutputStream outputStream = s.getOutputStream();
                InputStream inputStream = s.getInputStream();
        ){
            StorageMessages.AllChunks allChunks = StorageMessages.AllChunks.newBuilder()
                    .setGet(true)
                    .build();
            StorageMessages.DataPacket dataPacket = StorageMessages.DataPacket.newBuilder()
                    .setAllchunks(allChunks)
                    .build();
            dataPacket.writeDelimitedTo(outputStream);

            StorageMessages.DataPacket response = StorageMessages.DataPacket.getDefaultInstance();
            response = response.parseDelimitedFrom(inputStream);

            convert_from_chunk_store(response.getAllchunks());
            write_to_disk(response.getAllchunks());

        }catch (IOException | HashException ie){
            ie.getStackTrace();
        }

    }

    private class store_chunk_listener extends Thread{
        private Socket s;
        public store_chunk_listener(Socket s){
            this.s = s;
        }

        @Override
        public synchronized void run() {
            try {
                InputStream instream = s.getInputStream();
                OutputStream outputStream = s.getOutputStream();
                StorageMessages.DataPacket dataPacket = StorageMessages.DataPacket.parseDelimitedFrom(instream);

                if(dataPacket.hasRequest()) {
                    logger.debug("Request Recieved");
                    process_request(dataPacket.getRequest(), s);
                    send_ack(outputStream);
                }
                else if(dataPacket.hasHashringentry()) {
                    process_hre(dataPacket);
                    send_ack(outputStream);
                }else if(dataPacket.hasSinglechunk()){
                    process_single_chunk(dataPacket, s);
                    send_ack(outputStream);
                }else if(dataPacket.hasChunklife()){
                    send_ack(outputStream);
                    pipline_update(dataPacket);
                }else if(dataPacket.hasAllchunks()){
                    logger.debug("proccessing all chunks");
                    process_allchunks(dataPacket.getAllchunks(), s.getOutputStream());
                }else if(dataPacket.hasDiskspace()){
                    logger.debug("proccessing disk space");
                    process_disk_space(outputStream);
                }else if(dataPacket.hasNumberofrequest()){
                    process_requests_handled(outputStream);
                }else if(dataPacket.hasHashring())
                    process_hash_ring_request(s);
                number_request_handled += 1;
                s.close();
            }catch(IOException | HashException e) {
                e.printStackTrace();
            }
        }

    }

    private void send_ack(OutputStream outputStream) throws IOException {

        StorageMessages.Ack ack = StorageMessages.Ack.newBuilder().setAck(true).build();
        StorageMessages.DataPacket dataPacket = StorageMessages.DataPacket.newBuilder().setAck(ack).build();
        dataPacket.writeDelimitedTo(outputStream);
    }

    private void process_allchunks(StorageMessages.AllChunks allChunks, OutputStream outputStream) throws IOException {
        if(allChunks.getGet()){
            send_files(outputStream);
        }else
            get_chunk_map(outputStream);

    }

    private void process_hash_ring_request (Socket s) throws IOException {
        System.out.println("HashRing Request Recieved");
        System.out.println(hashRing.toString());
        CoordMessages.DataPacket dataPacket = CoordMessages.DataPacket.newBuilder()
                .setHashring(hashRing.treemap_to_map())
                .build();
        dataPacket.writeDelimitedTo(s.getOutputStream());
    }

    private void process_disk_space(OutputStream outputStream) throws IOException {
        long byte_len = new File("/").getFreeSpace();
        double gb = byte_len/1e+9;
        StorageMessages.DiskSpace diskSpace = StorageMessages.DiskSpace.newBuilder().setDiskspace(gb).build();
        StorageMessages.DataPacket dataPacket = StorageMessages.DataPacket.newBuilder().setDiskspace(diskSpace).build();
        dataPacket.writeDelimitedTo(outputStream);

    }

    private void process_requests_handled(OutputStream outputStream) throws IOException {
        StorageMessages.NumberOfRequest numberOfRequest = StorageMessages.NumberOfRequest.newBuilder()
                .setNumber(number_request_handled)
                .build();
        StorageMessages.DataPacket dataPacket = StorageMessages.DataPacket.newBuilder()
                .setNumberofrequest(numberOfRequest)
                .build();
        dataPacket.writeDelimitedTo(outputStream);
    }

    private void send_files(OutputStream outputStream) throws IOException {
        logger.debug("Sending files ");
        System.out.println("sending files");
        StorageMessages.AllChunks allChunks = StorageMessages.AllChunks.newBuilder()
                .putAllChunkMap(convert_to_chunk_store())
                .putAllFiles(convert_to_files())
                .build();
        StorageMessages.DataPacket dataPacket = StorageMessages.DataPacket.newBuilder()
                .setAllchunks(allChunks)
                .build();
        dataPacket.writeDelimitedTo(outputStream);
    }

    private void get_chunk_map(OutputStream outputStream) throws IOException{
        System.out.println("Converting chunk map");
        StorageMessages.AllChunks allChunks = StorageMessages.AllChunks.newBuilder()
                .putAllChunkMap(convert_to_chunk_store())
                .build();
        StorageMessages.DataPacket dataPacket = StorageMessages.DataPacket.newBuilder()
                .setAllchunks(allChunks)
                .build();
        dataPacket.writeDelimitedTo(outputStream);
    }

    private void convert_from_chunk_store(StorageMessages.AllChunks allChunks) throws HashException {
        for (Map.Entry<String,StorageMessages.SingleChunk> entry : allChunks.getChunkMap().entrySet()) {
            String key = entry.getKey();
            Chunk chunk = new Chunk(entry.getValue().getChecksum(), entry.getValue().getFileName(), entry.getValue().getChunkNumber(), entry.getValue().getIsLast());
            chunk_storage.put(key, chunk);
        }
        System.out.println(chunk_storage.size());
    }

    private void write_to_disk(StorageMessages.AllChunks allChunks) throws IOException, HashException {
        for( Map.Entry<String, ByteString> entry: allChunks.getFilesMap().entrySet()){
            String filename = entry.getKey();
            Path path = Paths.get(dir + "/" + filename);
            Files.write(path, entry.getValue().toByteArray());
            System.out.println(filename);

        }

    }


    private Map<String, StorageMessages.SingleChunk> convert_to_chunk_store()
    {
        Map<String, StorageMessages.SingleChunk> singleChunkMap = new HashMap<>();
        if(!chunk_storage.isEmpty()) {
            for (Map.Entry<String, Chunk> entry : chunk_storage.entrySet()) {
                String key = entry.getKey();
                StorageMessages.SingleChunk singleChunk = StorageMessages.SingleChunk.newBuilder()
                        .setChunkNumber(entry.getValue().getChunk_id())
                        .setIsLast(entry.getValue().getIs_last())
                        .setFileName(entry.getValue().getFile_name())
                        .setChecksum(entry.getValue().getChecksum())
                        .build();
                singleChunkMap.put(key, singleChunk);
            }
        }
        System.out.println(singleChunkMap.size());
        return singleChunkMap;
    }

    private Map<String, ByteString> convert_to_files() throws IOException {
        Map<String, ByteString> file_map = new HashMap<>();
        File folder = new File(path);
        File[] listOfFiles = folder.listFiles();
        if(listOfFiles.length != 0) {
            for (File file : listOfFiles) {
                if (file.isFile()) {
                    byte[] c_bytes = Files.readAllBytes(file.toPath());
                    ByteString bsval = ByteString.copyFrom(c_bytes, 0, c_bytes.length);
                    file_map.put(file.getName(), bsval);
                }
            }
        }
        return file_map;

    }

    /*
    Function used to proccess hash ring request
     */
    private void process_hre(StorageMessages.DataPacket dataPacket)
    {

        StorageMessages.HashRingEntry hashRingEntry = dataPacket.getHashringentry();
        try {
            if(hashRingEntry.getAdd()) {
                logger.debug("Adding a new node to the hash map");
                System.out.println("adding node");
                hashRing.addNodePos(new BigInteger(hashRingEntry.getPosition().toByteArray()), hashRingEntry.getIpaddress(), hashRingEntry.getPort());
            }
            else if(!hashRingEntry.getAdd()) {
                logger.debug("Removing a node from the hash map");
                System.out.println("removing node");
                hashRing.remove_node(new BigInteger(hashRingEntry.getPosition().toByteArray()));
            }
        } catch (HashTopologyException e) {
            e.printStackTrace();
        }
    }

    /*
    Request access from the coordinator.
     */
    private boolean request_access(String ipaddress, int port)
    {
        System.out.println("requesting access");
        try (
                Socket s = new Socket(ipaddress,port);
                OutputStream outputStream = s.getOutputStream();
                InputStream inputStream = s.getInputStream();
        ){
            CoordMessages.RequestEntry requestEntry = CoordMessages.RequestEntry.newBuilder()
                    .setIpaddress(this.ipaddress)
                    .setPort(this.port)
                    .build();
            CoordMessages.DataPacket dataPacket = CoordMessages.DataPacket.newBuilder()
                    .setRequestentry(requestEntry)
                    .build();
            dataPacket.writeDelimitedTo(outputStream);
            CoordMessages.DataPacket response = CoordMessages.DataPacket.getDefaultInstance();
            response = response.parseDelimitedFrom(inputStream);

            hashRing = new HashRing(sha1,response.getHashring());
            mypos = new BigInteger(response.getHashring().getHre().getPosition().toByteArray());
            System.out.println(hashRing.toString());
            System.out.println("My position in the hash ring is: " + mypos);
            return true;

        }catch (IOException ioe){
            ioe.printStackTrace();
        }

        return false;
    }

    /*
    Process request packets
    -   Storage packets
    -   Get requests
     */
    private void process_request(StorageMessages.Request r_chunk,Socket s) throws IOException, HashException {
        if(r_chunk.getOpcode() == StorageMessages.Request.Op_code.store_chunk) {
            try {
                store_chunk(r_chunk);
            } catch (HashException | IOException e) {
                e.printStackTrace();
            }
        }
        else if(r_chunk.getOpcode() == StorageMessages.Request.Op_code.get_chunk) {
            String key = key_gen(r_chunk.getFileName(),r_chunk.getChunkId(),r_chunk.getIslast());
            if(chunk_storage.containsKey(key))
                get_chunk(key, r_chunk.getIpaddress(),r_chunk.getPort());
            else
                forward_request(r_chunk);
        }


    }
    /*
    processes a single chunk to send back to cliet
     */
    public void get_chunk(String key,String ipaddress,int port) throws IOException{

        Chunk chunk = chunk_storage.get(key);
        Path file = Paths.get(dir + "/" + key);
        byte[] c_bytes = Files.readAllBytes(file);
        logger.debug("Checking data for corruption");
        if(!chunk.validate(c_bytes)) {
            logger.debug("Corruption detected");
            HashRingEntry hre = hashRing.get_next_entry(mypos);
            c_bytes = uncorrupt(hre,chunk);
        }else
            logger.debug("No Corruption detected");
        ByteString bsval = ByteString.copyFrom(c_bytes, 0, c_bytes.length);
        StorageMessages.SingleChunk singleChunk = StorageMessages.SingleChunk.newBuilder()
                .setChunkNumber(chunk.getChunk_id())
                .setFileName(chunk.getFile_name())
                .setIsLast(chunk.getIs_last())
                .setData(bsval)
                .build();
        StorageMessages.DataPacket dataPacket = StorageMessages.DataPacket.newBuilder()
                .setSinglechunk(singleChunk)
                .build();

        DataRequester dataRequester = new DataRequester(dataPacket, ipaddress, port);
        dataRequester.start();

    }

    private byte[] uncorrupt (HashRingEntry hre, Chunk chunk) throws IOException {
        Socket s = new Socket(hre.inetaddress,hre.port);
        OutputStream outputStream = s.getOutputStream();
        InputStream inputStream = s.getInputStream();
        StorageMessages.SingleChunk singleChunk = StorageMessages.SingleChunk.newBuilder()
                .setFileName(chunk.getFile_name())
                .setChunkNumber(chunk.getChunk_id())
                .setIsLast(chunk.getIs_last())
                .build();
        StorageMessages.DataPacket dataPacket = StorageMessages.DataPacket.newBuilder()
                .setSinglechunk(singleChunk)
                .build();

        dataPacket.writeDelimitedTo(outputStream);
        logger.debug("Contacting neighbor node for uncorrupted data");
        dataPacket = dataPacket.parseDelimitedFrom(inputStream);

        singleChunk = dataPacket.getSinglechunk();
        logger.debug("Data Recieved");
        String key = key_gen(singleChunk.getFileName(),singleChunk.getChunkNumber(),singleChunk.getIsLast());
        Path path = Paths.get(dir + "/" + key);
        Files.write(path, singleChunk.getData().toByteArray());
        logger.debug("Data Recoverved and rewritten \n\n\n");
        return singleChunk.getData().toByteArray();



    }


    public void store_chunk(StorageMessages.Request r_chunk) throws HashException, IOException {
        Chunk chunk = new Chunk(r_chunk.getData().toByteArray(),r_chunk.getFileName(),r_chunk.getChunkId(),r_chunk.getIslast());
        String key = key_gen(chunk.getFile_name(),chunk.getChunk_id(),chunk.getIs_last());
        BigInteger pos = hashRing.locate(key.getBytes());
        HashRingEntry node = hashRing.returnNode(pos);
        if(node.inetaddress.equals(this.ipaddress) && node.port == this.port) {
            chunk_storage.put(key, chunk);

            Path path = Paths.get(dir + "/" + key);
            Files.write(path, r_chunk.getData().toByteArray());

            StorageMessages.ChunkLife chunkLife = StorageMessages.ChunkLife.newBuilder()
                    .setSingleChunk(request_to_chunk(r_chunk))
                    .setLife(2)
                    .build();
            StorageMessages.DataPacket dataPacket = StorageMessages.DataPacket.newBuilder()
                    .setChunklife(chunkLife)
                    .build();
            HashRingEntry hre = get_next_neighbor(key,2);
            if(hre.inetaddress != this.ipaddress && hre.port != this.port) {
                DataRequesterWithAck dataRequester = new DataRequesterWithAck(dataPacket, hre.inetaddress, hre.port);
                dataRequester.start();
            }
        }
        else {
           forward_chunk(chunk,node,r_chunk);
        }
    }

    private HashRingEntry get_next_neighbor(String key,int life) throws HashException {
        BigInteger location = hashRing.locate(key.getBytes());
        HashRingEntry hre = null;
        for(int i = 1; i <= life; i++){
            hre = hashRing.get_next_entry(location);
            location = hre.position;
        }
        return hre;
    }

    private StorageMessages.SingleChunk request_to_chunk(StorageMessages.Request request)
    {
        return StorageMessages.SingleChunk.newBuilder()
                .setData(request.getData())
                .setWrite(true)
                .setIsLast(request.getIslast())
                .setFileName(request.getFileName())
                .setChunkNumber(request.getChunkId())
                .build();
    }

    private void forward_chunk(Chunk chunk, HashRingEntry node,StorageMessages.Request r_chunk) throws HashException{
        System.out.println("Storing on external node " + chunk.getFile_name() + Integer.toString(chunk.getChunk_id()));
        System.out.println(node.inetaddress + " " + Integer.toString(node.port));

        StorageMessages.DataPacket dataPacket = StorageMessages.DataPacket.newBuilder()
                .setRequest(r_chunk)
                .build();
        DataRequester dataRequester = new DataRequester(dataPacket,node.inetaddress,node.port);
        dataRequester.start();
    }

    private void forward_request(StorageMessages.Request r_chunk) throws HashException{
        StorageMessages.DataPacket dataPacket = StorageMessages.DataPacket.newBuilder()
                .setRequest(r_chunk)
                .build();
        String key = key_gen(r_chunk.getFileName(),r_chunk.getChunkId(),r_chunk.getIslast());
        BigInteger pos = hashRing.locate(key.getBytes());
        HashRingEntry hre = hashRing.returnNode(pos);
        DataRequester dataRequester = new DataRequester(dataPacket,hre.inetaddress,hre.port);
        dataRequester.start();
    }

    private void pipline_update(StorageMessages.DataPacket dataPacket) throws IOException {
        logger.debug("pipline updating chunk life : " + Integer.toString(dataPacket.getChunklife().getLife()));
        StorageMessages.ChunkLife chunkLife = dataPacket.getChunklife();
        chunkLife = StorageMessages.ChunkLife.newBuilder()
                .setLife(chunkLife.getLife() - 1)
                .setSingleChunk(chunkLife.getSingleChunk())
                .build();
        StorageMessages.SingleChunk singleChunk = chunkLife.getSingleChunk();
        Chunk chunk = new Chunk(singleChunk.getData().toByteArray(),singleChunk.getFileName(),singleChunk.getChunkNumber(),singleChunk.getIsLast());
        String key = key_gen(chunk.getFile_name(),chunk.getChunk_id(),chunk.getIs_last());
        if(!chunk_storage.containsKey(key)) {
            chunk_storage.put(key, chunk);
            Path path = Paths.get(dir + "/" + key);
            Files.write(path, singleChunk.getData().toByteArray());

            if (chunkLife.getLife() > 0) {
                StorageMessages.DataPacket sendpacket = StorageMessages.DataPacket.newBuilder().setChunklife(chunkLife).build();
                try {
                    HashRingEntry hre = get_next_neighbor(key, chunkLife.getLife());
                    DataRequesterWithAck dataRequester = new DataRequesterWithAck(sendpacket, hre.inetaddress, hre.port);
                    dataRequester.start();
                } catch (HashException e) {
                    e.printStackTrace();
                }
            }
        }


    }

    private String key_gen(String filename, int chunkid, boolean islast){
        if(islast)
            return filename + "last";
        else
            return filename + Integer.toString(chunkid);
    }

    private void process_single_chunk(StorageMessages.DataPacket dataPacket, Socket s){
        StorageMessages.SingleChunk singleChunk = dataPacket.getSinglechunk();
        try (
                OutputStream outputStream = s.getOutputStream();
        ) {

            String key = key_gen(singleChunk.getFileName(),singleChunk.getChunkNumber(),singleChunk.getIsLast());
            Chunk chunk = chunk_storage.get(key);

            Path file = Paths.get(dir + "/" + key);
            byte[] c_bytes = Files.readAllBytes(file);
            ByteString bsval = ByteString.copyFrom(c_bytes, 0, c_bytes.length);
            System.out.println("sending file: " + chunk.getFile_name());
            singleChunk = StorageMessages.SingleChunk.newBuilder()
                    .setChunkNumber(chunk.getChunk_id())
                    .setFileName(chunk.getFile_name())
                    .setIsLast(chunk.getIs_last())
                    .setData(bsval)
                    .build();
            dataPacket = StorageMessages.DataPacket.newBuilder()
                    .setSinglechunk(singleChunk)
                    .build();
            dataPacket.writeDelimitedTo(outputStream);
        }catch(IOException ioe){
            ioe.printStackTrace();
        }
    }




    public static void main(String[] args)
            throws Exception {
        String hostname = getHostname();
        System.out.println("Starting storage node on " + hostname + "...");
        StorageNode storageNode = new StorageNode(5100,"localhost",7000);
        storageNode.startNode();

    }



}
