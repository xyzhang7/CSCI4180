import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class MyDedup {

    private static final int FRINDEX_SIZE = 24;

    public static int RFP(byte[] f, int idx, int prevRFP, int m, int q, int d) {
        /*
         * m is the window size, min chunk size q is the modulo, usually set as a power
         * of 2 number, average chunk size d is the multiplier, 257 is a good choice for
         * binary file [(a mod n) * (b mod n)] mod n = (a * b) mod n [(a mod n) + (b mod
         * n)] mod n = (a + b) mod n
         */
        int dtemp = 1;
        int rt = 0;
        if (prevRFP == 0) {
            // The beginning of the file
            for (int i = idx + m - 1; i >= idx; i--) {
                if (i != idx + m - 1) {
                    dtemp = dtemp * d % q;
                }

                rt += (int) f[i] % q * dtemp;
                rt %= q;
            }
            return rt;

        } else {
            // Calculate by the previous rfp
            int tmp = (int) (Math.pow(d, m) % q);
            int dp = ((d % q) * (prevRFP % q)) % q;
            int dt = (((f[idx] & 0xFF) % q) * tmp) % q;
            int t = (f[idx + m - 1] & 0xFF % q);
            rt = ((dp - dt + t) % q);
        }

        return rt;
    }

    public static int isZeroChunk(byte[] f, int pos, int m) {
        int i = pos;
        int zeroNum = 0;
        while (i < f.length && f[i] == 0 ) {
            zeroNum = zeroNum + 1;
            i = i + 1;
        }
        // zeros chunks: zeros at the end of file
        if (pos + zeroNum >= f.length || zeroNum >= m) {
            return zeroNum;
        }
        return 0;

    }

    public static List<FRIndexItem> loadFRIndex(byte[] bytes) {
        List<FRIndexItem> frIndex = new ArrayList<>();

        if (bytes.length % FRINDEX_SIZE != 0) {
            System.out.println("Index length error.");
        }
        int num = bytes.length / FRINDEX_SIZE;
        for (int i = 0; i < num; i ++) {
            int offset = i * 24;
            FRIndexItem newfr = new FRIndexItem();

            newfr.setChecksum(Arrays.copyOfRange(bytes, offset, offset + 16));
            newfr.setUsed(((bytes[offset + 16] & 0xFF) << 24) | ((bytes[offset + 17] & 0xFF) << 16)
                    | ((bytes[offset + 18] & 0xFF) << 8) | ((bytes[offset + 19] & 0xFF)));

            newfr.setOffset(((bytes[offset + 20] & 0xFF) << 24) | ((bytes[offset + 21] & 0xFF) << 16)
                    | ((bytes[offset + 22] & 0xFF) << 8) | ((bytes[offset + 23] & 0xFF)));
            frIndex.add(newfr);

        }

        return frIndex;
    }

    public static List<FileRecipeItem> loadFileRecipe(byte[] bytes) {
        List<FileRecipeItem> fileRecipe = new ArrayList<>();
        int location = 0;

        while (location < bytes.length) {
            FileRecipeItem fileRecipeItem = new FileRecipeItem();
            fileRecipeItem.setIsZero(bytes[location]);
            location++;
            if (fileRecipeItem.getIsZero() == (byte) 0) {
                fileRecipeItem.setChecksum(Arrays.copyOfRange(bytes, location, location + 16));
                location += 16;
            }
            fileRecipeItem.setSize(((bytes[location] & 0xFF) << 24) | ((bytes[location + 1] & 0xFF) << 16)
                    | ((bytes[location + 2] & 0xFF) << 8) | ((bytes[location + 3] & 0xFF)));
            location += 4;
            fileRecipe.add(fileRecipeItem);
        }
        return fileRecipe;
    }

    public static Statistic loadStatistic() throws IOException, ClassNotFoundException {
        FileInputStream streamIn = new FileInputStream("./data/statistic");
        ObjectInputStream ois = new ObjectInputStream(streamIn);
        Statistic stat = null;
        stat = (Statistic) ois.readObject();
        if(stat != null){
            return stat;
        }else{
            System.out.println("Load Statistics Error");
            System.exit(1);
        }
        return stat;
    }

    public static void localWriteChunk(byte[] data, int name) throws Exception {
        File chunk = new File("./data/chunk-" + name);
        FileOutputStream os = new FileOutputStream(chunk);
        os.write(data);
        os.close();
    }

    public static void localWriteRecipe(String fileName, List<FileRecipeItem> recipes) throws IOException {
        // File dir = new File("data");

        // dir.mkdir();


        File recipe = new File("./data/recipe-" + fileName);
        FileOutputStream os = new FileOutputStream(recipe);
        for(FileRecipeItem item: recipes){
            os.write(item.getIsZero());

            if(item.getIsZero() ==(byte)0){
                os.write(item.getChecksum());
            }
            os.write(ByteBuffer.allocate(4).putInt(item.getSize()).array());
        }
        // os.flush();
        os.close();
    }

    public static void localWriteIndex(List<FRIndexItem> frIndex) throws Exception {
        File indexFile = new File("./data/mydedup.index");
        indexFile.delete();
        FileOutputStream os = new FileOutputStream(indexFile);

        for(FRIndexItem frIndexItem: frIndex){
            os.write(frIndexItem.toBytes());
        }

        os.close();
    }

    public static void localWriteStatistic(Statistic stat) throws Exception{
        File statFile = new File("./data/statistic");
        if(statFile.exists()){
            statFile.delete();
        }
        FileOutputStream os = new FileOutputStream(statFile);
        ObjectOutputStream oos = new ObjectOutputStream(os);
        oos.writeObject(stat);
        oos.close();
        os.close();
    }



    /* Upload */
    public static List<FileRecipeItem> chunk(int m, int q, int max, int d, byte[] f) throws IOException, NoSuchAlgorithmException {
        /* m is the window size, min chunk size
           q is the modulo, usually set as a power of 2 number, average chunk size
           d is the multiplier, 257 is a good choice for binary file
           f is the file
           pos is the position of latest anchor point + 1
           cnt is the processed bytes after latest anchor point
         */
        int prevRFP=0, pos=0, cnt=0, zeroNum=0;
        List<FileRecipeItem> fileRecipe = new ArrayList<FileRecipeItem>();

        while(pos < f.length){
//            System.out.println("In the loop. Postion: " + pos);
            zeroNum = isZeroChunk(f, pos, m);
            if(zeroNum != 0){ // handle zero chunk
                FileRecipeItem recipeItem = new FileRecipeItem();
                recipeItem.setIsZero((byte)1);
                recipeItem.setSize(zeroNum);
                fileRecipe.add(recipeItem);
                pos = pos + zeroNum;
                continue;
            }else if(pos + m >= f.length){ // File is not huge enough
                //Add checksum by MD5
                MessageDigest md = MessageDigest.getInstance("MD5");
                byte[] data = Arrays.copyOfRange(f, pos, f.length);
                md.update(data);
                byte[] checksumBytes = md.digest();

                FileRecipeItem recipeItem = new FileRecipeItem();
                recipeItem.setIsZero((byte)0);
                recipeItem.setChecksum(checksumBytes);
                recipeItem.setData(data);
                recipeItem.setSize(data.length);
                fileRecipe.add(recipeItem);

//                System.out.println("End. Data Length: " + data.length);
                break;
            }else{
                prevRFP = RFP(f, pos, 0, m, q, d);
                cnt = m;
                while(!((prevRFP & 0xFF) == 0 || cnt >= max || pos + m >= f.length)){
                    cnt ++;
                    prevRFP = RFP(f, pos + cnt - m, prevRFP, m, q, d);
                }
                MessageDigest md = MessageDigest.getInstance("MD5");
                byte[] data = Arrays.copyOfRange(f, pos, pos + cnt);
                md.update(data);
                byte[] checksumBytes = md.digest();
//                System.out.println(checksumBytes[0]);

                FileRecipeItem recipeItem = new FileRecipeItem();
                recipeItem.setIsZero((byte)0);
                recipeItem.setChecksum(checksumBytes);
                recipeItem.setSize(cnt);
                recipeItem.setData(data);
                fileRecipe.add(recipeItem);

//                System.out.println("Normal. Data Length: " + data.length);
                pos += cnt;
            }
        }

        return fileRecipe;
    }

    public static boolean checkFileExists(String filename){
        File file = new File(filename);
        boolean exists = file.exists();
        return exists;
    }

    /*
    public static int localCountFiles(){
        File directory = new File("./data");
        int cnt = 0;
        for(File file: directory.listFiles()){
            if(file.getName().contains("recipe")){
                cnt ++;
            }
        }
        return cnt;
    }
     */

    /* Download */
    public static void localDownload(String filename, List<FRIndexItem> frIndex, List<FileRecipeItem> fileRecipe)throws IOException {
        // List<Byte> data = new ArrayList<>();
        File newFile = new File(filename);
        FileOutputStream os = new FileOutputStream(newFile);

        for(FileRecipeItem fileRecipeItem: fileRecipe){

            if(fileRecipeItem.getIsZero() == 1){ // zero chunk
                int zerosize = fileRecipeItem.getSize();
                byte[] zerocontent = new byte[zerosize];
                for(int i = 0;i < zerosize;i++){
                    zerocontent[i] = (byte)0;
                }
                os.write(zerocontent);
            }else{// not zero
                for(FRIndexItem frIndexItem: frIndex){
                    if(Arrays.equals(fileRecipeItem.getChecksum(), frIndexItem.getChecksum())){

                        File chunkFile = new File("./data/chunk-" + frIndexItem.getOffset());
                        byte[] fileContent = Files.readAllBytes(chunkFile.toPath());
                        os.write(fileContent);
                    }
                }
            }

        }
        os.close();
    }

    /* Delete */
    public static void localDelete(String filename, List<FRIndexItem> frIndex, List<FileRecipeItem> fileRecipe, Statistic stat){
        int preChunk= fileRecipe.size();
        int uniChunk=0, preByte=0, uniByte=0;
        for(FileRecipeItem item: fileRecipe){
            preByte += item.getSize();

            if(item.getIsZero() == (byte) 0){

                for (Iterator<FRIndexItem> iter = frIndex.listIterator(); iter.hasNext();) {
                    FRIndexItem index = iter.next();
                    if (index.isDuplicated(item.getChecksum())) {
                        int used = index.getUsed() - 1;

                        //Remove the chunk if it is no longer being used
                        if( used == 0){
                            uniChunk++;
                            uniByte += item.getSize();
                            File chunk = new File("./data/chunk-" + index.getOffset());
                            if(!chunk.delete()){
                                System.out.println("Failed to delete the chunk file chunk-" + index.getOffset());
                            }
                            //Update the index file
                            iter.remove();
                        }else{
                            index.setUsed(used);
                        }
                    }
                }
            }
        }
        stat.setNumFile(stat.getNumFile() - 1);
        stat.setPreChunks(stat.getPreChunks() - preChunk);
        stat.setUniChunks(stat.getUniChunks() - uniChunk);
        stat.setPreBytes(stat.getPreBytes() - preByte);
        stat.setUniBytes(stat.getUniBytes() - uniByte);
        File recipe = new File("./data/recipe-" + filename);
        if(!recipe.delete()){
            System.out.println("Failed to delete the recipe file recipe-" + filename);
        }
    }

    public static void main(String[] args) throws Exception{
        if (args.length < 1) {
            //    usage();
            System.exit(1);
        }
        String operation = args[0];

        try{
            switch (operation) {
                case "upload": {
//                if (args.length < 3) {
//                    usage();
//                    System.exit(1);
//                }
                    int MIN_CHUNK_SIZE = Integer.parseInt(args[1]);
                    int AVG_CHUNK_SIZE = Integer.parseInt(args[2]);
                    int MAX_CHUNK_SIZE = Integer.parseInt(args[3]);
                    int d = Integer.parseInt(args[4]);
                    String fileName = args[5];
                    String option = args[6];

                    int UBytes = 0;

                    if(!checkFileExists(fileName)){
                        System.out.println("The file to upload does not exist");
                        System.exit(1);
                    }

                    Path p = FileSystems.getDefault().getPath("", fileName);
                    byte[] file = Files.readAllBytes(p);

                    if (option.equals("azure")) {
                        // TODO:
                    } else if (option.equals("local")) {
                        System.out.println("Process local upload");

                        if(checkFileExists("./data/recipe-" +fileName)){
                            System.out.println("File already exists");
                            System.exit(1);
                        }


                        File indexFile = new File("./data/mydedup.index");
                        List<FRIndexItem> frIndex;
                        if (indexFile.exists()) {
//                            System.out.println("Load FRIndex.");
                            byte[] fileContent = Files.readAllBytes(indexFile.toPath());
                            frIndex = loadFRIndex(fileContent);
                        } else {
                            frIndex = new ArrayList<>();
                        }
//                        System.out.println("Finished Index Reading. Size: " + frIndex.size());

                        List<FileRecipeItem> fileRecipes = chunk(MIN_CHUNK_SIZE, AVG_CHUNK_SIZE, MAX_CHUNK_SIZE, d, file);
//                        System.out.println("Finished gerenating file recipe. Size: " + fileRecipes.size());

                        for (FileRecipeItem fileRecipeItem : fileRecipes) {
                            if (fileRecipeItem.getIsZero() == (byte) 1) { // check zero chunk
                                continue;
                            }

                            boolean exist = false;
                            for (FRIndexItem indexItem : frIndex) { // check duplication
                                // if(indexItem.getChecksum().equals(fileRecipeItem.getChecksum())){
                                if (Arrays.equals(indexItem.getChecksum(), fileRecipeItem.getChecksum())) {
                                    exist = true;
                                    indexItem.setUsed(indexItem.getUsed() + 1);
//                                    System.out.println("There is duplication.");
                                    break;
                                }
                            }
                            if (!exist) { // a new chunk
                                FRIndexItem newItem = new FRIndexItem();
                                newItem.setChecksum(fileRecipeItem.getChecksum());
                                if (frIndex.size() == 0) {
                                    newItem.setOffset(0);
                                } else {
                                    newItem.setOffset(frIndex.get(frIndex.size() - 1).getOffset() + 1);
                                }

                                newItem.setUsed(1);
                                frIndex.add(newItem);
                                localWriteChunk(fileRecipeItem.getData(), newItem.getOffset());

                                UBytes += fileRecipeItem.getSize();
                            }
                        }
                        localWriteRecipe(fileName, fileRecipes);
                        localWriteIndex(frIndex);


                        //Report
                        Statistic stat;
                        if(checkFileExists("./data/statistic")){
                            stat = loadStatistic();
                        }else{
                            stat = new Statistic();
                        }
                        stat.setNumFile(stat.getNumFile() + 1);
                        stat.setPreChunks(stat.getPreChunks() + fileRecipes.size());
                        stat.setUniChunks(frIndex.size());
                        stat.setPreBytes(stat.getPreBytes() + file.length);
                        stat.setUniBytes(stat.getUniBytes() + UBytes);

                        stat.printStat();
                        localWriteStatistic(stat);

                    }


                    break;
                }
                case "download": {
                    String file_to_download = args[1];
                    String local_file_name = args[2];
                    String option = args[3];
                    if (option.equals("local")) {
                        if(!checkFileExists("./data/recipe-" + file_to_download)){
                            System.out.println("File does not exist");
                            System.exit(1);
                        }

                        File indexFile = new File("./data/mydedup.index");
                        List<FRIndexItem> frIndex;

                        byte[] indexContent = Files.readAllBytes(indexFile.toPath());
                        frIndex = loadFRIndex(indexContent);
                        System.out.println("Finished loading FRIndex. Size: " + frIndex.size());

                        File file = new File("./data/recipe-" + file_to_download);
                        List<FileRecipeItem> fileRecipe = new ArrayList<>();

                        byte[] fileContent = Files.readAllBytes(file.toPath());
                        fileRecipe = loadFileRecipe(fileContent);
                        System.out.println("Finished loading File recipe. Size: " + fileRecipe.size());

                        localDownload(local_file_name, frIndex, fileRecipe);

                    } else if (option.equals("azure")) {

                    }
                    break;
                }
                case "delete": {
                    String file_to_delete = args[1];
                    String option = args[2];
                    if (option.equals("local")) {
                        if(!checkFileExists("./data/recipe-" + file_to_delete)){
                            System.out.println("File does not exist");
                            System.exit(1);
                        }

                        File indexFile = new File("./data/mydedup.index");
                        List<FRIndexItem> frIndex;
                        byte[] indexContent = Files.readAllBytes(indexFile.toPath());
                        frIndex = loadFRIndex(indexContent);

                        File file = new File("./data/recipe-" + file_to_delete);
                        List<FileRecipeItem> fileRecipe = new ArrayList<>();
                        byte[] fileContent = Files.readAllBytes(file.toPath());
                        fileRecipe = loadFileRecipe(fileContent);

//                        System.out.println("Index File Before Delete:");
//
//                        for (FRIndexItem index : frIndex) {
//                            System.out.println(index.toString());
//                        }
                        Statistic stat = loadStatistic();
//                        stat.printStat();

                        localDelete(file_to_delete, frIndex, fileRecipe, stat);
//                        System.out.println("Index File After Delete:");
//                        for (FRIndexItem index : frIndex) {
//                            System.out.println(index.toString());
//                        }
//                        stat.printStat();

                        localWriteIndex(frIndex);
                        localWriteStatistic(stat);
                    }
                    break;
                }
            }

        }catch (Exception e){
            e.printStackTrace();
        }
    }

}

class FileRecipeItem{
    private byte[] checksum;
    private byte isZero;
    private int size;
    private byte[] data;

    public byte[] getChecksum() {
        return checksum;
    }
    public void setChecksum(byte[] checksum) {
        this.checksum = checksum;
    }
    public byte getIsZero() {
        return isZero;
    }
    public void setIsZero(byte isZero) {
        this.isZero = isZero;
    }
    public int getSize() {
        return size;
    }
    public void setSize(int size) {
        this.size = size;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    /*
    public byte[] toBytes(){
        if(this.isZero == (byte)0){
            byte[] ans = new byte[17];
            ans[0] = this.isZero;
            System.arraycopy(this.checksum, 0, ans, 1, 17);
            return ans;
        }else{
            byte[] ans = new byte[5];
            ans[0] = this.isZero;
            ans[1] = (byte)(this.size >>> 24);
            ans[2] = (byte)(this.size >>> 16);
            ans[3] = (byte)(this.size >>> 8);
            ans[4] = (byte)(this.size);
            return ans;
        }
    }
    */
}

class FRIndexItem{
    private byte[] checksum; //16
    private int used; //4
    private int offset; //4
    // private short size;

    public byte[] getChecksum() {
        return checksum;
    }

    public void setChecksum(byte[] checksum) {
        this.checksum = checksum;
    }

    public int getUsed() {
        return used;
    }

    public void setUsed(int used) {
        this.used = used;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public FRIndexItem(){
    }

    /*
    public FRIndexItem(byte[] checksum, int used, int offset ){
        this.checksum = checksum;
        this.used = used;
        this.offset = offset;
    }
    */

    public boolean isDuplicated(byte[] input){
        return Arrays.equals(input, this.checksum);
    }

    public byte[] toBytes(){
        byte[] bytes = new byte[24];
        System.arraycopy(this.checksum, 0, bytes, 0, 16);
        bytes[16] = (byte)(this.used >>> 24);
        bytes[17] = (byte)(this.used >>> 16);
        bytes[18] = (byte)(this.used >>> 8);
        bytes[19] = (byte)(this.used);


        bytes[20] = (byte)(this.offset >>> 24);
        bytes[21] = (byte)(this.offset >>> 16);
        bytes[22] = (byte)(this.offset >>> 8);
        bytes[23] = (byte)(this.offset);

        return bytes;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("\tFRIndexItem{");
        sb.append("checksum=").append(Arrays.toString(checksum));
        sb.append(", used=").append(used);
        sb.append(", offset=").append(offset);
        sb.append('}');
        return sb.toString();
    }
}

class Statistic implements Serializable{
    private int numFile;
    private int preChunks;
    private int uniChunks;
    private long preBytes;
    private long uniBytes;

    public void setNumFile(int numFile) {
        this.numFile = numFile;
    }

    public void setPreChunks(int preChunks) {
        this.preChunks = preChunks;
    }

    public void setUniChunks(int uniChunks) {
        this.uniChunks = uniChunks;
    }

    public void setPreBytes(long preBytes) {
        this.preBytes = preBytes;
    }

    public void setUniBytes(long uniBytes) {
        this.uniBytes = uniBytes;
    }

    public int getNumFile() {
        return numFile;
    }

    public int getPreChunks() {
        return preChunks;
    }

    public int getUniChunks() {
        return uniChunks;
    }

    public long getPreBytes() {
        return preBytes;
    }

    public long getUniBytes() {
        return uniBytes;
    }

    public void printStat(){
        System.out.println("Total number of files that have been stored: " + this.numFile);
        System.out.println("Total number of pre-deduplicated chunks in storage: " + this.preChunks
                + "\nTotal number of unique chunks in storage: " + this.uniChunks
                + "\nTotal number of bytes of pre-deduplicated chunks in storage: " + this.preBytes
                + "\nTotal number of bytes of unique chunks in storage: " + this.uniBytes
        );
        if(this.uniBytes != 0){
            System.out.println("Deduplication ratio: " + (float) (this.preBytes / this.uniBytes));
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Statistic{");
        sb.append("numFile=").append(numFile);
        sb.append(", preChunks=").append(preChunks);
        sb.append(", uniChunks=").append(uniChunks);
        sb.append(", preBytes=").append(preBytes);
        sb.append(", uniBytes=").append(uniBytes);
        sb.append('}');
        return sb.toString();
    }
}
