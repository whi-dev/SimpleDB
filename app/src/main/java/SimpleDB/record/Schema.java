package SimpleDB.record;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.sql.Types.INTEGER;
import static java.sql.Types.VARCHAR;
/**
 * レコードの情報を保持するクラス
 */
public class Schema {

  class FieldInfo {
    int type, length;
    public FieldInfo(int type, int length) {
      this.type = type;
      this.length = length;
    }
  }

  private List<String> fields = new ArrayList<>();
  private Map<String, FieldInfo> info = new HashMap<>();
  /**
   * 型と長さを指定してフィールドを追加する
   * @param fldname
   * @param type
   * @param length
   */
  public void addField(String fldname, int type, int length) {
    fields.add(fldname);
    info.put(fldname, new FieldInfo(type, length));
  }
  /**
   * int型のフィールドを追加する 
   * @param fldname
   */
  public void addIntfield(String fldname) {
    addField(fldname, INTEGER, 0);
  }
  /**
   * String型のフィールドを追加する
   * @param fldname
   * @param length
   */
  public void addStringField(String fldname, int length) {
   addField(fldname, VARCHAR, length); 
  }
  /**
   * スキーマのフィールドを指定してフィールドを追加する
   * @param fldname
   * @param sch
   */
  public void add(String fldname, Schema sch) {
    int type = sch.type(fldname);
    int length = sch.length(fldname);
    addField(fldname, type, length);
  }
  /**
   * スキーマのフィールドをすべて追加する
   * @param sch
   */
  public void addAll(Schema sch) {
    for(String fldname : sch.fields()) {
      add(fldname, sch);
    }
  }
  /**
   * フィールド名をすべて取得
   * @return
   */
  public List<String> fields() {
    return fields;
  }
  /**
   * フィールド名の型を取得
   * @param field
   * @return
   */
  public int type(String field) {
    return info.get(field).type;
  }
  /**
   * フィールドの長さを取得
   * @param field
   * @return
   */
  public int length(String field) {
    return info.get(field).type;
  }
  /**
   * フィールドが存在するか
   * @param field
   * @return
   */
  public boolean hasField(String field) {
    return info.containsKey(field);
  }
}
