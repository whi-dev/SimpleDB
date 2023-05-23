package SimpleDB.tx.recovery;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import SimpleDB.buffer.Buffer;
import SimpleDB.buffer.BufferManager;
import SimpleDB.file.BlockID;
import SimpleDB.log.LogManager;
import SimpleDB.tx.Transaction;
import SimpleDB.tx.recovery.LogRecord.LogType;
/**
 * データファイルの完全性を管理するオブジェクト
 * 操作ログの出力と、ファイルへの書き込みを管理する
 * Undoオンリーリカバリ方式を採用している
 */
public class RecoveryManager {
  private LogManager lm;
  private BufferManager bm;
  private Transaction tx;
  private int txnum;
  
  public RecoveryManager(Transaction tx, int txnum, LogManager lm, BufferManager bm) {
    this.tx = tx;
    this.txnum = txnum;
    this.lm = lm;
    this.bm = bm;
    StartRecord.writeToLog(lm, txnum);
  }
  /**
   * メモリ上の変更をディスクに書き込み、コミットレコードのLSNまでのログをログファイルに書き込む
   */
  public void commit() {
    bm.flushAll(txnum);
    int lsn = CommitRecord.writeToLog(lm, txnum);
    lm.flush(lsn);
  }
  /**
   * ロールバックを実行して、メモリ上の変更をディスクに書き込み、ロールバックレコードのLSNまでのログをログファイルに書き込む
   * @throws Exception
   */
  public void rollback() throws Exception{
    doRollback();
    bm.flushAll(txnum);
    int lsm = RollbackRecord.writeToLog(lm, txnum);
    lm.flush(lsm);
  }
  /**
   * ファイルの回復を実行して、チェックポイントレコードをログファイルに書き込む
   * @throws Exception
   */
  public void recover() throws Exception{
    doRecover();
    bm.flushAll(txnum);
    int lsn = CheckPointRecord.writeToLog(lm);
    lm.flush(lsn);
  }
  /**
   * 数値設定のログレコードを発行する
   * @param buf
   * @param offset
   * @param newval
   * @return
   * @throws Exception
   */
  public int setInt(Buffer buf, int offset, int newval) throws Exception{
    int oldval = buf.contents().getInt(offset);
    BlockID blk = buf.block();
    return SetIntLogRecord.writeToLog(lm, txnum, blk, offset, oldval);
  }
  /**
   * 文字列設定のログレコードを発行する
   * @param buf
   * @param offset
   * @param newval
   * @return
   */
  public int setString(Buffer buf, int offset, String newval) {
    String oldval = buf.contents().getString(offset);
    BlockID blk = buf.block();
    return SetStringLogRecord.writeToLog(lm, txnum, blk, offset, oldval);
  }
  private void doRollback() throws Exception{
    Iterator<byte[]> iter = lm.iterator();
    while(iter.hasNext()) {
      byte[] bytes = iter.next();
      LogRecord rec = LogRecord.creatLogRecord(bytes);
      if(rec.txNumber() == txnum) {
        if(rec.op() == LogRecord.LogType.START) {
          return;
        }
        rec.undo(tx);
      }
    }
  }
  private void doRecover() throws Exception{
    Collection<Integer> finishedTx = new ArrayList<>();
    Iterator<byte[]> iter = lm.iterator();
    while(iter.hasNext()) {
      byte[] bytes = iter.next();
      LogRecord rec = LogRecord.creatLogRecord(bytes);
      if(rec.op() == LogType.CHECKPOINT) {
        return;
      }
      if(rec.op() == LogType.COMMIT || rec.op() == LogType.ROLLBACK) {
        finishedTx.add(rec.txNumber());
      }
      else if(!finishedTx.contains(rec.txNumber())) {
        rec.undo(tx);
      }
    }
  }
}
