package SimpleDB.tx.concurrency;

import SimpleDB.exception.LockAbortException;
import SimpleDB.file.BlockID;

import java.util.Map;
import java.util.HashMap;

public class LockTable {
  private final int MAX_TIME = 10000;
  private Map<BlockID, Integer> locks = new HashMap<>();

  public synchronized void sLock(BlockID blk) throws Exception{
    try {
      long timestamp = System.currentTimeMillis();
      while(hasXLock(blk) && waitingTooLong(timestamp)) {
        wait(MAX_TIME);
      }
      if(hasXLock(blk)) {
        throw new LockAbortException();
      }
      int val = getLockVal(blk);
      locks.put(blk, val+1);
    } catch (Exception e) {
      throw e;
    }
  }
  public synchronized void xLock(BlockID blk) throws Exception{
    try {
      long timestamp = System.currentTimeMillis();
      while(hasOtherSLock(blk) && waitingTooLong(timestamp)) {
        wait(MAX_TIME);
      }
      if(hasOtherSLock(blk)) {
        throw new LockAbortException();
      }
      locks.put(blk, -1);
    } catch (Exception e) {
      throw e;
    }
  }
  synchronized void unlock(BlockID blk) {
    int val = getLockVal(blk);
    if (val > 1) {
      locks.put(blk, val-1);
    }
    else {
       locks.remove(blk);
       notifyAll();
    }
 }

  private boolean hasXLock(BlockID blk) {
    return getLockVal(blk) < 0;
  }
  private boolean hasOtherSLock(BlockID blk) {
    return getLockVal(blk) > 1;
  }
  private int getLockVal(BlockID blk) {
    Integer v = locks.get(blk);
    return v == null ? 0 : v.intValue();
  }
  private boolean waitingTooLong(long starttime) {
    return System.currentTimeMillis() - starttime > MAX_TIME;
 }
}
