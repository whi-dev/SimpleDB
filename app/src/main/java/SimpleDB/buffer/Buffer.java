package SimpleDB.buffer;

import SimpleDB.file.BlockID;
import SimpleDB.file.FileManager;
import SimpleDB.file.Page;
import SimpleDB.log.LogManager;

public class Buffer {
  private FileManager fm;
  private LogManager lm;
  private Page contents;
  private BlockID blk = null;
  private int pins = 0;
  private int txnum = -1;
  private int lsn = -1;

  public Buffer(FileManager fm, LogManager lm) {
    this.fm = fm;
    this.lm = lm;
    contents = new Page(fm.blockSize());
  }
  public Page contents() {
    return contents;
  }
  public BlockID block() {
    return blk;
  }
  public void setModified(int txnum, int lsn) {
    this.txnum = txnum;
    if(lsn >= 0) {
      this.lsn = lsn;
    }
  }
  public boolean isPinned() {
    return pins > 0;
  }
  public int modifyingTx() {
    return txnum;
  }
  void assignToBlock(BlockID blk) {
    flush();
    this.blk = blk;
    fm.read(blk, contents);
    pins = 0;
  }
  void flush() {
    if(txnum >= 0) {
      lm.flush(lsn);
      fm.write(blk, contents);
      txnum = -1;
    } 
  }
  void pin() {
    pins++;
  }
  void unpin() {
    pins--;
  }
}
