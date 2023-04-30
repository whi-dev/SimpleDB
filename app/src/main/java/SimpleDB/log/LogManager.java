package SimpleDB.log;

import java.util.Iterator;

import SimpleDB.file.BlockID;
import SimpleDB.file.FileManager;
import SimpleDB.file.Page;

public class LogManager {
  private FileManager fm;
  private String logfile;
  private Page logpage;
  private BlockID currentblk;
  private int latestLSN;
  private int lastSavedLSN;

  public LogManager(FileManager fm, String logfile) {
    this.fm = fm;
    this.logfile = logfile;
    byte[] b = new byte[fm.blockSize()];
    logpage = new Page(b);
    int logsize = fm.length(logfile);
    if(logsize == 0) {
      currentblk = appendNewBlock();
    }
    else {
      currentblk = new BlockID(logfile, logsize-1);
      fm.read(currentblk, logpage);
    }
  }
  public void flush(int lsn) {
    if(lsn >= lastSavedLSN) {
      flush();
    }
  }
  public Iterator<byte[]> iterator() {
    flush();
    return new LogIterator(fm, currentblk);    
  }
  public synchronized int append(byte[] logrec) {
    int boundary = logpage.getInt(0);
    int recordsize = logrec.length;
    int bytesneeded = recordsize + Integer.BYTES; // レコードの必要バイト+長さを記録するバイト
    int recpos = boundary - recordsize;
    // レコードはページの後方追記されており、先頭には最後に追加されたレコードのオフセットが書かれている
    // 従って、可能な次のレコード書き込み位置は４バイト以降である
    if(recpos < Integer.BYTES) {
      // 次のブロックに移動する
      currentblk = appendNewBlock();
      boundary = logpage.getInt(0);
      recpos = boundary - recordsize; // 書き込み位置を再計算
    }
    logpage.setBytes(recpos, logrec);
    logpage.setInt(0, recpos); // update boundary
    ++latestLSN;
    return latestLSN;
  }
  private void flush() {
    fm.write(currentblk, logpage);
    lastSavedLSN = latestLSN;
  }
  private BlockID appendNewBlock() {
    BlockID blk = fm.append(logfile);
    logpage.setInt(0, fm.blockSize());
    fm.write(blk, logpage);
    return blk;
  }

}
