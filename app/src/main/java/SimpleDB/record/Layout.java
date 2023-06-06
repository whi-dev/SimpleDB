package SimpleDB.record;

import java.util.HashMap;
import java.util.Map;

import SimpleDB.file.Page;

import static java.sql.Types.*;

public class Layout {
  private Schema sch;
  private Map<String, Integer> offsets;
  private int slotsize;

  public Layout(Schema sch) {
    this.sch = sch;
    offsets = new HashMap<>();
    int pos = Integer.BYTES; // スロットの先頭はスロットが空いているかの情報
    for(String fldname : sch.fields()) {
      offsets.put(fldname, pos);
      pos += lengthInBytes(fldname);
    }
  }
  public Layout(Schema sch, Map<String, Integer> offsets, int slotsize) {
    this.sch = sch;
    this.offsets = offsets;
    this.slotsize = slotsize;
  }
/**
 * スキーマを取得
 * @return
 */
  public Schema schema() {
    return sch;
  }
  /**
   * fldnameに対応するオフセットを取得
   * @param fldname
   * @return
   */
  public int offset(String fldname) {
    return offsets.get(fldname);
  }
  /**
   * スロットサイズを取得
   * @return
   */
  public int slotSize() {
    return slotsize;
  }
  /**
   * フィールドのバイト数を計算
   * @param fldname
   * @return
   */
  private int lengthInBytes(String fldname) {
    int fldtype = sch.type(fldname);
    if(fldtype == INTEGER) {
      return Integer.BYTES;
    }
    else {
      return Page.maxLength(sch.length(fldname));
    }
  }
  
}
