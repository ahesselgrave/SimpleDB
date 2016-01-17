package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    private File mFile;
    private TupleDesc mTupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
	mFile = f;
	mTupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
	return mFile;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
	return mFile.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.

     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
	return mTupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
	try {
	    RandomAccessFile raf = new RandomAccessFile(mFile, "r");
	    int offset = BufferPool.PAGE_SIZE * pid.pageNumber();
	    
	    raf.seek(offset);
	    byte[] data = new byte[BufferPool.PAGE_SIZE];
	    
	    //check if we go over
	    if (BufferPool.PAGE_SIZE + offset > raf.length()) {
		throw new IllegalArgumentException(String.format("Invalid pid, offset %d exceeds file page count %d", offset, numPages()));
	    }

	    for (int i = 0; i < data.length; i++) {
		data[i] = raf.readByte();
	    }

	    Page page = new HeapPage((HeapPageId)pid, data);
	    return page;
	} catch (Exception e) {
	    e.printStackTrace();
	    throw new IllegalArgumentException("Something went wrong with pid" + pid.toString());
	}
	
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
	return (int) mFile.length() / BufferPool.PAGE_SIZE;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
	DbFileIterator iter = new HeapFileIterator(this, tid, numPages());
	return iter;
    }

}

