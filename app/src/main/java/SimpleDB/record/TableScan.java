package SimpleDB.record;

import static java.sql.Types.INTEGER;
import static java.sql.Types.VARCHAR;

import SimpleDB.file.BlockID;
import SimpleDB.tx.Transaction;
import SimpleDB.query.Constant;
import SimpleDB.query.UpdateScan;

//! UpdateScanは後の章ででてくるらしい
public class TableScan implements UpdateScan {
  private Transaction tx;
  private Layout layout;
  private RecordPage rp;
  private String filename;
  private int currentslot;

  public TableScan(Transaction tx, String tblname, Layout layout) throws Exception{
    this.tx = tx;
    this.filename = tblname + ".tbl";
    if (tx.size(filename) == 0) {
      moveToNewBlock();
    } else {
      moveToBlock(0);
    }
  }

  /**
   * 現在のレコードブロックのスキャンを終了する
   */
  public void close() {
    if (rp != null) {
      tx.unpin(rp.block());
    }
  }

  /**
   * ファイルの先頭に移動する
   */
  public void beforeFirst() throws Exception{
    moveToBlock(0);
  }

  /**
   * 次のデータがあるスロットに移動する
   * 
   * @return true:次のデータが存在する
   */
  public boolean next() throws Exception{
    currentslot = rp.nextAfter(currentslot);
    while (currentslot < 0) {
      if (atLastBlock()) {
        return false;
      }
      moveToBlock(rp.block().number() + 1);
      currentslot = rp.nextAfter(currentslot);
    }
    return true;
  }

  /**
   * 現在指しているスロットからfldnameの数値を取得する
   * 
   * @param fldname
   * @return
   */
  public int getInt(String fldname) throws Exception{
    return rp.getInt(currentslot, fldname);
  }

  /**
   * 現在指しているスロットからfldnameの文字列の値を取得する
   * 
   * @param fldname
   * @return
   */
  public String getString(String fldname) throws Exception{
    return rp.getString(currentslot, fldname);
  }

  /**
   * 現在のスロットからfldnameの値を取得する
   * 
   * @param fldname
   * @return
   */
  public Constant getVal(String fldname) throws Exception{
    if(layout.schema().type(fldname) == INTEGER) {
      return new Constant(getInt(fldname));
    }
    else {
      return new Constant(getString(fldname));
    }
  }

  /**
   * 現在のスロットより後の空いているスロットに移動して使用済みにする。
   * 空いているスロットがない場合はファイルにブロックを追加する。
   */
  public void insert() throws Exception{
    currentslot = rp.insertAfter(currentslot);
    while (currentslot < 0) {
      if (atLastBlock()) {
        moveToNewBlock();
        ;
      } else {
        moveToBlock(rp.block().number() + 1);
      }
      currentslot = rp.insertAfter(currentslot);
    }
  }

  /**
   * 現在のスロットを空にする
   */
  public void delete() throws Exception{
    rp.delete(currentslot);
  }

  /**
   * ridが指すブロックIDのスロットに移動する
   * 
   * @param rid
   */
  public void moveToRid(RID rid) throws Exception{
    moveToBlock(rid.blockNumber());
    currentslot = rid.slot();
  }

  /**
   * 現在の指しているRIDを取得する
   * 
   * @return
   */
  public RID getRid() {
    return new RID(rp.block().number(), currentslot);
  }

  /**
   * ファイルの最後のブロックか？
   * 
   * @return
   */
  private boolean atLastBlock() throws Exception {
    return rp.block().number() == tx.size(filename) - 1;
  }

  /**
   * データファイルのblknumが指すブロックに移動する
   * 
   * @param blknum
   */
  private void moveToBlock(int blknum) throws Exception{
    close();
    BlockID blk = new BlockID(filename, blknum);
    rp = new RecordPage(tx, blk, layout);
    currentslot = -1;
  }

  /**
   * データファイルに新しいブロックを追加して、そのブロックに移動する
   */
  private void moveToNewBlock() throws Exception{
    close();
    BlockID blk = tx.append(filename);
    rp = new RecordPage(tx, blk, layout);
    rp.format();
    currentslot = -1;
  }
}
