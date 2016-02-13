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
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            throw new IllegalArgumentException("Something went wrong with pid" + pid.toString());
        } catch (IOException e) {
            e.printStackTrace();
            throw new IllegalArgumentException("Something went wrong with pid" + pid.toString());
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        PageId pageId = page.getId();
        int offset = BufferPool.PAGE_SIZE * pageId.pageNumber();

        RandomAccessFile raf = new RandomAccessFile(mFile, "rw");
        raf.seek(offset);

        raf.write(page.getPageData());
        raf.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
	return (int) mFile.length() / BufferPool.PAGE_SIZE;
    }

    /**
     *     @see DbFile for javadocs
     */

    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        HeapPage emptyHeapPage = null;
        for (int i = 0; i < numPages(); i++) {
            PageId pid = new HeapPageId(getId(), i);
            HeapPage hp = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);

            if (hp.getNumEmptySlots() > 0) {
                emptyHeapPage = hp;
                break;
            }
        }

        ArrayList<Page> modifiedPages = new ArrayList<>();

        if (emptyHeapPage != null) {
            emptyHeapPage.insertTuple(t);
            modifiedPages.add(emptyHeapPage);
        } else {
            // Have to create a new page and add it to the file
            HeapPageId hpid = new HeapPageId(getId(), numPages());
            HeapPage heapPage = new HeapPage(hpid, new byte[BufferPool.getPageSize()]);
            heapPage.insertTuple(t);

            // Use random access file to go to end of mFile to append the page
            RandomAccessFile raf = new RandomAccessFile(mFile, "rw");
            raf.seek(mFile.length());
            byte[] heapPageData = heapPage.getPageData();
            raf.write(heapPageData, 0, BufferPool.PAGE_SIZE);
            raf.close();

            // Get return arraylist of modified pages
            modifiedPages.add(heapPage);
        }

        return modifiedPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        RecordId rid = t.getRecordId();
        HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, rid.getPageId(), Permissions.READ_WRITE);
        heapPage.deleteTuple(t);

        ArrayList<Page> modifiedPages = new ArrayList<>();
        modifiedPages.add(heapPage);
        return modifiedPages;
    }

    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this, tid);
    }

    /**
     * Helper class that implements the Java Iterator for tuples on a HeapFile
     */
    class HeapFileIterator extends AbstractDbFileIterator {
        Iterator<Tuple> mTupleIterator;
        int mCurrentPageNumber;
        TransactionId mTid;
        HeapFile mHeapFile;

        /**
         * Set local variables for HeapFile and Transactionid
         *
         * @param hf
         *            The underlying HeapFile.
         * @param tid
         *            The transaction ID.
         */
        public HeapFileIterator(HeapFile hf, TransactionId tid) {
            mHeapFile = hf;
            mTid = tid;
        }


        public void open() throws DbException, TransactionAbortedException {
            mCurrentPageNumber = -1;
        }

        @Override
        protected Tuple readNext() throws TransactionAbortedException,
                DbException {

            // If the current tuple iterator has no more tuples.
            if (mTupleIterator != null && !mTupleIterator.hasNext()) {
                mTupleIterator = null;
            }

            // Keep trying to open a tuple iterator until we find one of run out of pages.
            while (mTupleIterator == null
                    && mCurrentPageNumber < mHeapFile.numPages() - 1) {
                mCurrentPageNumber++; // Go to next page.

                // Get the iterator for the current page
                HeapPageId currentPageId = new HeapPageId(mHeapFile.getId(),
                        mCurrentPageNumber);

                HeapPage currentPage = (HeapPage) Database.getBufferPool()
                        .getPage(mTid, currentPageId, Permissions.READ_ONLY);
                mTupleIterator = currentPage.iterator();

                // Make sure the iterator has tuples in it
                if (!mTupleIterator.hasNext())
                    mTupleIterator = null;
            }

            // Make sure we found a tuple iterator
            if (mTupleIterator == null)
                return null;

            // Return the next tuple.
            return mTupleIterator.next();
        }

        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        public void close() {
            super.close();
            mTupleIterator = null;
            mCurrentPageNumber = Integer.MAX_VALUE;
        }
    }

}

