package SimpleDB.file;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Date;

public class Page {
  private ByteBuffer bb;
  private int blockSize;
  public static final Charset CHARSET = StandardCharsets.US_ASCII;

  public Page(int blocksize) {
    bb = ByteBuffer.allocateDirect(blocksize);
    this.blockSize = blocksize;
  }
  public Page(byte[] b) {
    bb = ByteBuffer.wrap(b);
  }
  public int getInt(int offset) {
    return bb.getInt(offset);
  }
  public void setInt(int offset, int n) {
    bb.putInt(offset, n);
  }
  public short getShort(int offset) {
    return bb.getShort(offset);
  }
  public void setShort(int offset, short n) {
    bb.putShort(offset, n);
  }
  public boolean getBoolean(int offset) {
    char b = bb.getChar(offset);
    return b == '\0';
  }
  public void setBoolean(int offset, boolean b) {
    char c = b ? 1 : '\0';
    bb.putChar(offset, c);
  }
  public Date getDate(int offset) {
    long epoch = bb.getLong(offset);
    return new Date(epoch);
  }
  public void setDate(int offset, Date d) {
    bb.putLong(offset, d.getTime());  
  }
  public byte[] getBytes(int offset) {
    bb.position(offset);
    int length = bb.getInt();
    byte[] b = new byte[length];
    bb.get(b);
    return b;
  }
  public void setBytes(int offset, byte[] b) throws RuntimeException{
    if(offset + b.length > blockSize) {
      throw new RuntimeException("overflow");
    }
    bb.position(offset);
    bb.putInt(b.length);
    bb.put(b);
  }
  public String getString(int offset) {
    byte[] str = new byte[blockSize];
    int idx = 0;
    char c;
    while((c = bb.getChar(offset++)) != '\0') {
      str[idx++] = (byte)c;
    }
    return new String(str, 0, idx, CHARSET);
  }
  public void setString(int offset, String s) throws RuntimeException{
    byte[] b = s.getBytes(CHARSET);
    if(offset + b.length > blockSize) {
      throw new RuntimeException("overflow");
    }
    bb.position(offset);
    bb.put(b);
    bb.putChar(offset + b.length, '\0');
  }

  public static int maxLength(int strlen) {
    float bytesPerChar = CHARSET.newEncoder().maxBytesPerChar();
    // length + btyes of String
    return Integer.BYTES + (strlen * (int)bytesPerChar);    
  }
  ByteBuffer contents() {
    bb.position(0);
    return bb;
  }
}