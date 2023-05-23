package SimpleDB.file;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;
/**
 * ファイルを管理するクラス
 * ファイルへのブロック操作を提供する
 */
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
  /**
   * ブロックの内容をファイルに読み込む
   * @param blk
   * @param p
   */
  public synchronized void read(BlockID blk, Page p) {
    try{
      RandomAccessFile f = getFile(blk.fileName());
      f.seek(blk.number() * blockSize);
      f.getChannel().read(p.contents());
    }catch(IOException e) {
      throw new RuntimeException("cannot read block " + blk);
    }
  }
  /**
   * ページの内容をブロックに書き込む
   * @param blk
   * @param p
   */
  public synchronized void write(BlockID blk, Page p)
  {
    try{
      RandomAccessFile f = getFile(blk.fileName());
      f.seek(blk.number() * blockSize);
      f.getChannel().write(p.contents());
    }catch(IOException e) {
      throw new RuntimeException("cannot write block " + blk);
    }
  }
  /**
   * ファイル末尾にブロックを追加する
   * @param filename
   * @return
   */
  public synchronized BlockID append(String filename) {
    int newBlkIdx = length(filename);
    BlockID blk = new BlockID(filename, newBlkIdx);
    byte[] b = new byte[blockSize];
    try {
      RandomAccessFile f = getFile(blk.fileName());
      f.seek(blk.number() * blockSize);
      f.write(b);
    }catch(IOException e) {
      throw new RuntimeException("cannot append block " + blk);
    }
    return blk;
  }
  /**
   * ファイルのブロック数を取得する
   * @param filename
   * @return
   */
  public int length(String filename) {
    try {
      RandomAccessFile f = getFile(filename);
      return (int)(f.length() / blockSize);
    }catch(IOException e) {
      throw new RuntimeException("cannot access " + filename);
    }
  }
  public boolean isNew() {
    return isNew;
  }
  public int blockSize() {
    return blockSize;    
  }
  private RandomAccessFile getFile(String filename) throws IOException{
    RandomAccessFile f = openFiles.get(filename);
    if(f == null) {
      File dbTable = new File(dbDirectory, filename);
      f = new RandomAccessFile(dbTable, "rws");
      openFiles.put(filename, f);
    }
    return f;
  }
}