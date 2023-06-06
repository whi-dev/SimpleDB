package SimpleDB.record;

import SimpleDB.file.BlockID;
import SimpleDB.tx.Transaction;

import static java.sql.Types.INTEGER;
import static java.sql.Types.VARCHAR;

public class RecordPage {
  public static final int EMPTY = 0, USED = 1;
  private Transaction tx;
  private BlockID blk;
  private Layout layout;

  public RecordPage(Transaction tx, BlockID blk, Layout layout) {
    this.tx = tx;
    this.blk = blk;
    this.layout = layout;
  }
  public int getInt(int slot, String fldname) throws Exception{
    int fldpos = offset(slot) + layout.offset(fldname);
    return tx.getInt(blk, fldpos);
  }
  public String getString(int slot, String fldname) throws Exception{
    int fldpos = offset(slot) + layout.offset(fldname);
    return tx.getString(blk, fldpos);
  }
  public void setInt(int slot, String fldname, int val) throws Exception{
    int fldpos = offset(slot) + layout.offset(fldname);
    tx.setInt(blk, fldpos, val, true);
  }
  public void setString(int slot, String fldname, String val) throws Exception{
    int fldpos = offset(slot) + layout.offset(fldname);
    tx.setString(blk, fldpos, val, true);
  }
  public void delete(int slot) throws Exception{
    setFlag(slot, EMPTY);
  }
  /**
   * ページをフォーマットする
   * @throws Exception
   */
  public void format() throws Exception{
    int slot = 0;
    while(isValidSlot(slot)) {
      tx.setInt(blk, offset(slot), EMPTY, false);
      Schema s = layout.schema();
      for(String fldname : s.fields()) {
        int fldpos = offset(slot) + layout.offset(fldname);
        if(s.type(fldname) == INTEGER) {
          tx.setInt(blk, fldpos, 0, false);
        }
        else {
          tx.setString(blk, fldpos, "", false);
        }
      }
      slot++;
    }
  }
  /**
   * 次に出現する使用済みスロットを取得する。見つからなかったら-1
   * @param slot
   * @return
   * @throws Exception
   */
  public int nextAfter(int slot) throws Exception{
    return searchAfter(slot, USED);
  }
  /**
   * 次に出現する空スロットを使用済みにする。見つからなかったら-1
   * @param slot
   * @return
   * @throws Exception
   */
  public int insertAfter(int slot) throws Exception{
    int newslot = searchAfter(slot, EMPTY);
    if(newslot >= 0) {
      setFlag(newslot, USED);
    }
    return newslot;
  }
  public BlockID block() {
    return blk;
  }
  /**
   * slotより後のスロットからflag状態のスロットを探す。見つからなかったら-1
   * @param slot
   * @param flag
   * @return
   * @throws Exception
   */
  private int searchAfter(int slot, int flag) throws Exception{
    slot++;
    while(isValidSlot(slot)) {
      if(tx.getInt(blk, offset(slot)) == flag) {
        return slot;
      }
      slot++;
    }
    return -1;
  }
  /**
   * スロットがレコードページに収まるか
   * @param slot
   * @return
   */
  private boolean isValidSlot(int slot) {
    return offset(slot+1) <= tx.blockSize();
  }
  /**
   * スロットに利用フラグを設定する
   * @param slot
   * @param flag
   * @throws Exception
   */
  private void setFlag(int slot, int flag) throws Exception{
    tx.setInt(blk, offset(slot), flag, true);
  }
  /**
   * 与えられたスロットの開始オフセットを返す
   * @param slot
   * @return
   */
  private int offset(int slot) {
    return layout.slotSize() * slot;
  }
}
