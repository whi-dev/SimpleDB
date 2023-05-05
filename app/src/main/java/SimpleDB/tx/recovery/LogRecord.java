package SimpleDB.tx.recovery;

import SimpleDB.file.Page;
import SimpleDB.tx.Transaction;

public interface LogRecord {
  enum LogType{
    CHECKPOINT,
    START,
    COMMIT,
    ROLLBACK,
    SETINT,
    SETSTRING,
  }

  LogType op();

  int txNumber();

  void undo(Transaction tx);

  static LogRecord creatLogRecord(byte[] bytes) {
    Page p = new Page(bytes);
    LogType t = LogType.values()[p.getInt(0)];
    switch(t) {
      case CHECKPOINT:
        return new CheckPointRecord();
      case START:
        return new StartRecord(p);
      case COMMIT:
        return new CommitRecord(p);
      case ROLLBACK:
        return new RollbackRecord(p);
      case SETINT:
        return new SetIntLogRecord(p);
      case SETSTRING:
        return new SetStringLogRecord(p);
      default:
        return null;
    }
  }

  public static int txpos() {
    return Integer.BYTES;
  }
}
