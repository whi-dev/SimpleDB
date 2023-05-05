package SimpleDB.tx.recovery;

import SimpleDB.tx.Transaction;

import SimpleDB.file.Page;
import SimpleDB.log.LogManager;

public class CheckPointRecord implements LogRecord{
  public CheckPointRecord() {
  }

  @Override
  public LogType op() {
    return LogType.CHECKPOINT;
  }

  @Override
  public int txNumber() {
    return -1;
  }

  @Override
  public void undo(Transaction tx) {
  }

  public String toString() {
    return "<CHECKPOINT>";
  }

  public static int writeToLog(LogManager lm) {
    byte[] rec = new byte[Integer.BYTES];
    Page p = new Page(rec);
    p.setInt(0, LogType.CHECKPOINT.ordinal());
    return lm.append(rec);
  }

}
