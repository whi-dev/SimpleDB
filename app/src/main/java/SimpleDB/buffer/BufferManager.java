package SimpleDB.buffer;

import SimpleDB.log.LogManager;

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

}
