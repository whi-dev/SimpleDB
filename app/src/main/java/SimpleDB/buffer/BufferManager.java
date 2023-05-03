package SimpleDB.buffer;

import SimpleDB.log.LogManager;

import java.util.HashMap;
import java.util.Map;

import SimpleDB.exception.BufferAbortException;
import SimpleDB.file.BlockID;
import SimpleDB.file.FileManager;

public class BufferManager {
  private class CandidateBuffer {
    public Buffer b;
    public boolean modified;
    public long latestPinned;
    public CandidateBuffer() {
      this.b = null;
      this.modified = true;
      latestPinned = Long.MAX_VALUE;
    }
  }
  private Buffer[] bufferpool;
  private Map<BlockID, Buffer> pinnedBufs;
  private Map<BlockID, Buffer> unpinnedBufs;
  private int numAvailable;
  private static final long MXA_TIME = 10000;


  public BufferManager(FileManager fm, LogManager lm, int numbuffs) {
    bufferpool = new Buffer[numbuffs];
    numAvailable = numbuffs;
    pinnedBufs = new HashMap<>(numbuffs);
    unpinnedBufs = new HashMap<>(numbuffs);
    for (int i = 0; i < numbuffs; ++i) {
      bufferpool[i] = new Buffer(fm, lm);
    }

  }
  public synchronized int available() {
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
      pinnedBufs.remove(buf.block());
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
      assignToBlock(buf, blk);
    }
    if(!buf.isPinned()) {
      numAvailable--;
    }
    buf.pin();
    return buf;
  }
  private Buffer findExistingBuffer(BlockID blk) {
    return pinnedBufs.get(blk);
  }
  private Buffer chooseUnpinnedBuffer(BlockID blk) {
    CandidateBuffer candidate = new CandidateBuffer();
    if((candidate.b = unpinnedBufs.get(blk)) != null) {
      return candidate.b;
    }
    for(var buf : bufferpool) {
      if(!buf.isPinned()) {
        if(candidate.modified == buf.isModified() && 
           candidate.latestPinned > buf.latestPinned()) {
          candidate.b = buf;
          candidate.latestPinned = buf.latestPinned();
        }
        else if(candidate.modified && !buf.isModified()){
          candidate.b = buf;
          candidate.latestPinned = buf.latestPinned();
          candidate.modified = buf.isModified();
        }
      }
    }
    return candidate.b;
  }
  private void assignToBlock(Buffer buf, BlockID blk) {
    unpinnedBufs.remove(blk);
    buf.assignToBlock(blk);
    pinnedBufs.put(blk, buf);
  }
}
