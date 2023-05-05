package SimpleDB.tx.concurrency;

import java.util.HashMap;
import java.util.Map;

import SimpleDB.file.BlockID;

public class ConcurrencyManager {
  private static LockTable locktable = new LockTable();
  private Map<BlockID, String> locks = new HashMap<>();

  public void sLock(BlockID blk) throws Exception{
    if(locks.get(blk) == null) {
      locktable.sLock(blk);
      locks.put(blk, "S");
    }
  }
  public void xLock(BlockID blk) throws Exception{
    if(hasXLock(blk)) {
      sLock(blk);
      locktable.sXock(blk);
      locks.put(blk, "X");
    }
  }
  public void release() {
    for(var blk : locks.keySet()) {
      locktable.unlock(blk);
    }
    locks.clear();
  }

  private boolean hasXLock(BlockID blk) {
    var locktype = locks.get(blk);
    return locktype != null && locktype.equals("X");
  }
}
