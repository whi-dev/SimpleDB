package SimpleDB.tx.recovery;

import SimpleDB.file.BlockID;
import SimpleDB.file.Page;
import SimpleDB.log.LogManager;
import SimpleDB.tx.Transaction;

public class SetIntLogRecord implements LogRecord {
  private int txnum, offset, prevVal;
  private BlockID blk;
  public SetIntLogRecord(Page p) {
    int pos = LogRecord.txpos();
    txnum = p.getInt(pos);
    {
      String filename = p.getString(pos += Integer.BYTES);
      int blknum = p.getInt(pos += Page.maxLength(filename.length()));
      blk = new BlockID(filename, blknum);
    }
    offset = p.getInt(pos += Integer.BYTES);
    prevVal = p.getInt(pos += Integer.BYTES);
  }

  @Override
  public LogType op() {
    return LogType.SETINT;
  }

  @Override
  public int txNumber() {
    return txnum;
  }

  @Override
  public void undo(Transaction tx) {
    tx.pin(blk);
    tx.setInt(blk, offset, prevVal, false);
    tx.unpin(blk);
  }

  @Override
  public String toString() {
    return "<SETSTRING " + txnum + " " + blk + " " + offset + " " + prevVal + ">";
  }

  public static int writeToLog(LogManager lm, int txnum, BlockID blk, int offset, int val) {
    int tpos = Integer.BYTES;
    int fpos = tpos + Integer.BYTES;
    int bpos = fpos + Page.maxLength(blk.fileName().length());
    int opos = bpos + Integer.BYTES;
    int vpos = opos + Integer.BYTES;
    byte[] rec = new byte[vpos + Integer.BYTES];
    Page p = new Page(rec);
    p.setInt(0, LogType.SETINT.ordinal());
    p.setInt(tpos, txnum);
    p.setString(fpos, blk.fileName());
    p.setInt(bpos, blk.number());
    p.setInt(opos, offset);
    p.setInt(vpos, val);
    return lm.append(rec);
  }
}
