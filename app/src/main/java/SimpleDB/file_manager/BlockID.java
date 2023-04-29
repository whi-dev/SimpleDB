package SimpleDB.file_manager;

public class BlockID {
  private String filename;
  private int blknum;

  public BlockID(String filename, int blknum) {
    this.filename = filename;
    this.blknum = blknum;
  }
  public String fileName() {
    return filename;
  }
  public int number() {
    return blknum;
  }
  public boolean equals(Object obj) {
    BlockID other = (BlockID) obj;
    return filename.equals(other.fileName());
  }
  public String toString() {
    return "[file " + filename + ", block " + blknum + "]";
  }
  public int hashCode() {
    return toString().hashCode();
  }
}