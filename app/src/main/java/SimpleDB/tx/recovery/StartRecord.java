package SimpleDB.tx.recovery;

import SimpleDB.file.Page;
import SimpleDB.log.LogManager;
import SimpleDB.tx.Transaction;

public class StartRecord implements LogRecord {
   private int txnum;
   
   /**
    * Create a log record by reading one other value from the log.
    * @param bb the bytebuffer containing the log values
    */
   public StartRecord(Page p) {
      int tpos = Integer.BYTES;
      txnum = p.getInt(tpos);
   }
   
   public LogType op() {
      return LogType.START;
   }
   
   public int txNumber() {
      return txnum;
   }
   
   /**
    * Does nothing, because a start record
    * contains no undo information.
    */
   public void undo(Transaction tx) {}
   
   public String toString() {
      return "<START " + txnum + ">";
   }
   
   /** 
    * A static method to write a start record to the log.
    * This log record contains the START operator,
    * followed by the transaction id.
    * @return the LSN of the last log value
    */
   public static int writeToLog(LogManager lm, int txnum) {
      byte[] rec = new byte[2*Integer.BYTES];
      Page p = new Page(rec);
      p.setInt(0, LogType.START.ordinal());
      p.setInt(Integer.BYTES, txnum);
      return lm.append(rec);
   }
}
