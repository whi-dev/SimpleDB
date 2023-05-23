package SimpleDB.log;

import java.util.Iterator;

import SimpleDB.file.BlockID;
import SimpleDB.file.FileManager;
import SimpleDB.file.Page;

/**
 * ログファイルを管理するオブジェクト
 * ログファイルへの操作を提供する
 */
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
  /**
   * 与えられたログシーケンスナンバーまでのログをディスクに書き込む
   * @param lsn
   */
  public void flush(int lsn) {
    if(lsn >= lastSavedLSN) {
      flush();
    }
  }
  public synchronized Iterator<byte[]> iterator() {
    flush();
    return new LogIterator(fm, currentblk);    
  }
  /**
   * ログレコードを追加する
   * 現在のブロックに十分な空きがない場合は、新しくブロックを追加して、
   * @param logrec
   * @return
   */
  public synchronized int append(byte[] logrec) {
    int boundary = logpage.getInt(0);
    int recordsize = logrec.length;
    int bytesneeded = recordsize + Integer.BYTES; // レコードの必要バイト+長さを記録するバイト
    int recpos = boundary - bytesneeded;
    // レコードはページの後方から追記されており、ページの先頭には最後に追加されたレコードの先頭位置（オフセット）が書かれている
    // 従って、可能な次のレコードの書き込み位置は５バイト目以降である
    if(recpos < Integer.BYTES) {
      // 次のブロックに移動する
      currentblk = appendNewBlock();
      boundary = logpage.getInt(0);
      recpos = boundary - bytesneeded; // 書き込み位置を再計算
    }
    logpage.setBytes(recpos, logrec);
    logpage.setInt(0, recpos); // update boundary
    ++latestLSN;
    return latestLSN;
  }
  /**
   * ログをディスクに書き込む
   */
  private void flush() {
    fm.write(currentblk, logpage);
    lastSavedLSN = latestLSN;
  }
  /**
   * ログファイルに新しいブロックを追加する
   * ブロックの先頭4バイトに最初のログ書き込み境界であるブロックサイズを書き込む
   * @return
   */
  private BlockID appendNewBlock() {
    BlockID blk = fm.append(logfile);
    logpage.setInt(0, fm.blockSize());
    fm.write(blk, logpage);
    return blk;
  }

}
