package SimpleDB.buffer;

import SimpleDB.log.LogManager;
import SimpleDB.exception.BufferAbortException;
import SimpleDB.file.BlockID;
import SimpleDB.file.FileManager;

public class BufferManager {
  private Buffer[] bufferpool;
  private int numAvailable;
  private static final long MXA_TIME = 10000;

  public BufferManager(FileManager fm, LogManager lm, int numbuffs) {
    bufferpool = new Buffer[numbuffs];
    numAvailable = numbuffs;
    for (int i = 0; i < numbuffs; ++i) {
      bufferpool[i] = new Buffer(fm, lm);
    }
  }
  public synchronized int  available() {
    return numAvailable;
  }
  public synchronized void flushAll(int txnum) {
    for(Buffer buff : bufferpool) {
      if(buff.modifyingTx() == txnum) {
        buff.flush();
      }
    }
  }
  public synchronized void unpin(Buffer buf) {
    buf.unpin();
    if(!buf.isPinned()) {
      numAvailable++;
      notifyAll();
    }
  }
  public synchronized Buffer pin(BlockID blk) throws InterruptedException{
    try {
      long timestamp = System.currentTimeMillis();
      Buffer buf = tryToPin(blk);
      if(buf == null && waitTooLong(timestamp)) {
        wait(MXA_TIME);
        tryToPin(blk);
      }
      if(buf == null) {
        throw new BufferAbortException(); 
      }
      return buf;
    } catch(InterruptedException e) {
      throw new BufferAbortException(); 
    }
  }
  private boolean waitTooLong(long timestamp) {
    return System.currentTimeMillis() - timestamp > MXA_TIME;
  }
  private Buffer tryToPin(BlockID blk) {
    Buffer buf = findExistingBuffer(blk);
    if(buf == null) {
      buf = chooseUnpinnedBuffer(blk);
      if(buf == null) {
        return null;
      }
      buf.assignToBlock(blk);
    }
    if(!buf.isPinned()) {
      numAvailable--;
    }
    buf.pin();
    return buf;
  }
  private Buffer findExistingBuffer(BlockID blk) {
    for(var buf : bufferpool) {
      BlockID b = buf.block();
      if(b != null && b.equals(blk)) {
        return buf;
      }
    }
    return null;
  }
  private Buffer chooseUnpinnedBuffer(BlockID blk) {
    for(var buf : bufferpool) {
      if(!buf.isPinned()) {
        return buf;
      }
    }
    return null;
  }

}
