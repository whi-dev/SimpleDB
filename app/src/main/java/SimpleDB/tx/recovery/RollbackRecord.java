package SimpleDB.tx.recovery;

import SimpleDB.file.Page;
import SimpleDB.log.LogManager;
import SimpleDB.tx.Transaction;

public class RollbackRecord implements LogRecord {
  private int txnum;
  public RollbackRecord(Page p) {
    txnum = p.getInt(LogRecord.txpos());
  }

  @Override
  public LogType op() {
    return LogType.ROLLBACK;
  }

  @Override
  public int txNumber() {
    return txnum;
  }

  @Override
  public void undo(Transaction tx) {
  }

  @Override
  public String toString() {
    return "<ROLLBACK " + txnum + ">";
  }

  public static int writeToLog(LogManager lm, int txnum) {
    byte[] rec = new byte[2*Integer.BYTES];
    Page p = new Page(rec);
    p.setInt(0, LogType.ROLLBACK.ordinal());
    p.setInt(Integer.BYTES, txnum);
    return lm.append(rec);
  }

}
