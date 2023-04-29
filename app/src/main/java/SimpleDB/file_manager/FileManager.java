package SimpleDB.file_manager;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;

public class FileManager {
  private File dbDirectory;
  private int blockSize;
  private boolean isNew;
  private Map<String, RandomAccessFile> openFiles = new HashMap<>();

  public FileManager(File dbDirectory, int blockSize)
  {
    this.dbDirectory = dbDirectory;
    this.blockSize = blockSize;
    this.isNew = !dbDirectory.exists();

    if(isNew) {
      dbDirectory.mkdirs();
    }
    for(String filename : dbDirectory.list()) {
      if(filename.startsWith("temp")) {
        new File(dbDirectory, filename).delete();
      }
    }
  }
  public synchronized void read(BlockID id, Page p) {
    
  }

}