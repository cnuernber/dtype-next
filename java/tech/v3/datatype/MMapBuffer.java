package tech.v3.datatype;


import java.io.File;
import java.io.FileDescriptor;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
/* import sun.misc.SharedSecrets; */
import xerial.larray.impl.LArrayNative;
import xerial.larray.impl.OSInfo;
import xerial.larray.impl.*;
import xerial.larray.mmap.*;

/**
 * Memory-mapped file buffer
 *
 * Original Author - @author Taro L. Saito
 * Changed to work with java14 - Chris Nuernberger
 */
public class MMapBuffer {

    private final RandomAccessFile raf;
    private final FileChannel fc;
    private final long fd;
    private final int pagePosition;


    public final long address;
    public final long mapSize;

    /**
     * Open an memory mapped file.
     * @param f
     * @param mode
     * @throws IOException
     */
    public MMapBuffer(File f, MMapMode mode) throws IOException {
        this(f, 0L, f.length(), mode);
    }

    /**
     * Open an memory mapped file.
     * @param f
     * @param offset
     * @param size
     * @param mode
     * @throws IOException
     */
    public MMapBuffer(File f, long offset, long size, MMapMode mode) throws IOException {
        super();
        this.raf = new RandomAccessFile(f, mode.mode);
        this.fc = raf.getChannel();
        // Retrieve file descriptor
        FileDescriptor rawfd = raf.getFD();
        try {
            if(!OSInfo.isWindows()) {
                Field idf = rawfd.getClass().getDeclaredField("fd");
                idf.setAccessible(true);
                this.fd = idf.getInt(rawfd);
            }
            else {
                // In Windows, fd is stored as 'handle'
                Field idf = rawfd.getClass().getDeclaredField("handle");
                idf.setAccessible(true);
                this.fd = idf.getLong(rawfd);
            }
        }
        catch(Exception e) {
            throw new IOException("Failed to retrieve file descriptor of " + f.getPath() + ": " + e.getMessage());
        }

        long allocationGranule = UnsafeUtil.unsafe.pageSize();
        this.pagePosition = (int) (offset % allocationGranule);

        // Compute mmap address
        if(!fc.isOpen())
            throw new IOException("closed " + f.getPath());

        long fileSize = fc.size();
        if(fileSize < offset + size) {
            // If file size is smaller than the specified size, extend the file size
            raf.seek(offset + size - 1);
            raf.write(0);
            //logger.trace(s"extend file size to ${fc.size}")
        }
        long mapPosition = offset - pagePosition;
        mapSize = size + pagePosition;
        // A workaround for the error when calling fc.map(MapMode.READ_WRITE, offset, size) with size more than 2GB

        long rawAddr = LArrayNative.mmap(fd, mode.code, mapPosition, mapSize);
        //trace(f"mmap addr:$rawAddr%x, start address:${rawAddr+pagePosition}%x")

        this.address = rawAddr + pagePosition;
    }

    /**
     * Close the memory mapped file. To ensure the written data is saved in the file, call flush before closing.
     */
    public void close() throws IOException {
        fc.close();
    }

    protected long offset() {
        return pagePosition;
    }

}
