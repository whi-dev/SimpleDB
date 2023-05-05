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
  public void commit() {
    bm.flushAll(txnum);
    int lsn = CommitRecord.writeToLog(lm, txnum);
    lm.flush(lsn);
  }
  public void rollback() {
    doRollback();
    bm.flushAll(txnum);
    int lsm = RollbackRecord.writeToLog(lm, txnum);
    lm.flush(lsm);
  }
  public void recover() {
    doRecover();
    bm.flushAll(txnum);
    int lsn = CheckPointRecord.writeToLog(lm);
    lm.flush(lsn);
  }
  public int setInt(Buffer buf, int offset, int newval) {
    int oldval = buf.contents().getInt(offset);
    BlockID blk = buf.block();
    return SetIntLogRecord.writeToLog(lm, txnum, blk, offset, oldval);
  }
  public int setString(Buffer buf, int offset, String newval) {
    String oldval = buf.contents().getString(offset);
    BlockID blk = buf.block();
    return SetStringLogRecord.writeToLog(lm, txnum, blk, offset, oldval);
  }
  private void doRollback() {
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
  private void doRecover() {
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
