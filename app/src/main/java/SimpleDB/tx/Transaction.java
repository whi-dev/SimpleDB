package SimpleDB.tx;

import SimpleDB.buffer.Buffer;
import SimpleDB.buffer.BufferManager;
import SimpleDB.file.BlockID;
import SimpleDB.file.FileManager;
import SimpleDB.file.Page;
import SimpleDB.log.LogManager;
import SimpleDB.tx.concurrency.ConcurrencyManager;
import SimpleDB.tx.recovery.RecoveryManager;

public class Transaction {
    private static int nextTxNum = 0;
    private static final int END_OF_FILE = -1;
    private RecoveryManager rm;
    private ConcurrencyManager cm;
    private BufferManager bm;
    private FileManager fm;
    private int txnum;
    private BufferList bl;

    public Transaction(FileManager fm, LogManager lm, BufferManager bm) {
        this.fm = fm;
        this.bm = bm;
        this.txnum = nextTxNumber();
        this.rm = new RecoveryManager(this, txnum, lm, bm);
        this.cm = new ConcurrencyManager();
        this.bl = new BufferList(bm);
    }
    public void commit() {
        rm.commit();
        cm.release();
        bl.unpinAll();
        System.out.println("transaction " + txnum + " committed");
    }
    public void rollback() throws Exception{
        rm.rollback();
        cm.release();
        bl.unpinAll();
        System.out.println("transaction " + txnum + " rolled back"); 
    }
    public void recover() throws Exception{
        bm.flushAll(txnum);
        rm.recover();
    }

    public void pin(BlockID blk) throws InterruptedException{
        bl.pin(blk);
    }
    
    public void unpin(BlockID blk) {
        bl.unpin(blk);
    }

    public int getInt(BlockID blk, int offset) throws Exception{
        cm.sLock(blk);
        var buf = bl.getBuffer(blk);
        return buf.contents().getInt(offset);
    }

    public String getString(BlockID blk, int offset) throws Exception{ 
        cm.sLock(blk); 
        Buffer buff = bl.getBuffer(blk); 
        return buff.contents().getString(offset); 
    }

    public void setInt(BlockID blk, int offset, int val, boolean okToLog) throws Exception{
        cm.xLock(blk);
        Buffer buf = bl.getBuffer(blk);
        int lsn = -1;
        if(okToLog) {
            lsn = rm.setInt(buf, offset, val);
        }
        Page p = buf.contents();
        p.setInt(offset, val);
        buf.setModified(txnum, lsn);
    }

    public void setString(BlockID blk, int offset, String val, boolean okToLog) throws Exception{
        cm.xLock(blk);
        Buffer buf = bl.getBuffer(blk);
        int lsn = -1;
        if(okToLog) {
            lsn = rm.setString(buf, offset, val);
        }
        Page p = buf.contents();
        p.setString(offset, val);
        buf.setModified(txnum, lsn);
    }
    public int size(String filename) throws Exception{
        BlockID dummyBlk = new BlockID(filename, END_OF_FILE);
        cm.sLock(dummyBlk);
        return fm.length(filename);
    }
    public BlockID append(String filename) throws Exception{
        BlockID dummyBlk = new BlockID(filename, END_OF_FILE);
        cm.xLock(dummyBlk);
        return fm.append(filename);
    }
    public int blockSize() {
        return fm.blockSize();
    }
    public int availableBuffers() {
        return bm.available();
    }
    private static synchronized int nextTxNumber() {
        nextTxNum++;
        System.out.println("new transaction: " + nextTxNum);
        return nextTxNum;
    }

  
}
