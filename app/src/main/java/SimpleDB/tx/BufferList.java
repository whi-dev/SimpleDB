package SimpleDB.tx;

import SimpleDB.buffer.Buffer;
import SimpleDB.buffer.BufferManager;
import SimpleDB.file.BlockID;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BufferList {
    private Map<BlockID, Buffer> buffers = new HashMap<BlockID, Buffer>();
    private List<BlockID> pins = new ArrayList<BlockID>();
    private BufferManager bm;

    public BufferList (BufferManager bm) {
        this.bm = bm;
    }

    public Buffer getBuffer(BlockID blk) {
        Buffer buff = buffers.get(blk);
        return buff;
    }
    void pin(BlockID blk) throws InterruptedException{
        Buffer buff = bm.pin(blk);
        buffers.put(blk, buff);
        pins.add(blk);  
    }
    void unpin(BlockID blk) {
        Buffer buff = buffers.get(blk);
        bm.unpin(buff);
        pins.remove(blk);
        if (!pins.contains(blk)) {
            buffers.remove(blk);
        }
    }
    void unpinAll() {
        for(BlockID blk : pins) {
            Buffer buf = buffers.get(blk);
            bm.unpin(buf);
        }
        buffers.clear();
        pins.clear();
    }
}
