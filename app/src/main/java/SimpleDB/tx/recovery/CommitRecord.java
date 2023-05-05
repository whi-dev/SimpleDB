package SimpleDB.tx.recovery;

import SimpleDB.file.Page;
import SimpleDB.log.LogManager;
import SimpleDB.tx.Transaction;

public class CommitRecord implements LogRecord {
  private int txnum; 

  public CommitRecord(Page p) {
    txnum = p.getInt(LogRecord.txpos());
  }

  @Override
  public LogType op() {
    return LogType.COMMIT;
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
    return "<COMMIT " + txnum + ">";
  }

  public static int writeToLog(LogManager lm, int txnum) {
    byte[] rec = new byte[2*Integer.BYTES];
    Page p = new Page(rec);
    p.setInt(0, LogType.COMMIT.ordinal());
    p.setInt(LogRecord.txpos(), txnum);
    return lm.append(rec);
  }

}
