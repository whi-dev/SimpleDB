package SimpleDB.log;

import java.util.Iterator;

import SimpleDB.file.BlockID;
import SimpleDB.file.FileManager;
import SimpleDB.file.Page;

public class LogIterator implements Iterator<byte[]>{
  private FileManager fm;
  private BlockID blk;
  private Page p;
  private int currentpos;
  private int boundary;

  public LogIterator(FileManager fm, BlockID blk) {
    this.fm = fm;
    this.blk = blk;
    byte[] b = new byte[fm.blockSize()];
    p = new Page(b);
    moveToBlock(blk);
  }
  @Override
  public boolean hasNext() {
    // イテレータはより古い方にしかイテレーションできない
    return currentpos < fm.blockSize() || blk.number() > 0;
  }
  @Override
  public byte[] next() {
    // ページで最も古いレコードを読み込むとポジションはblockSizeになる
    if(currentpos == fm.blockSize()) {
      // 一つ前のブロックをページに読み込む
      blk = new BlockID(blk.fileName(), blk.number()-1);
      moveToBlock(blk);
    }
    byte[] rec = p.getBytes(currentpos);
    currentpos = rec.length + Integer.BYTES;
    return rec;
  }
  private void moveToBlock(BlockID b) {
    fm.read(b, p);;
    //currentpos = p.getInt(0);
    boundary = p.getInt(0);
    currentpos = boundary;
  }
}
