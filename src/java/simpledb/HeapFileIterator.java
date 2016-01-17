package simpledb;

import java.io.*;
import java.util.*;

public class HeapFileIterator implements DbFileIterator {
    private Iterator<Tuple> mTupleIterator;
    private HeapFile mHeapFile;
    private TransactionId mTransactionId;
    private PageId mPageId;
    private int mPageNo = 0;
    private final int mMaxPages;
    private boolean mIsClosed = true;

    public HeapFileIterator(HeapFile hf, TransactionId tid, int maxPages){
	mHeapFile = hf;
	mTransactionId = tid;
	mPageId = new HeapPageId(mHeapFile.getId(), mPageNo);
	mMaxPages = maxPages;
    }

    public void open() throws DbException, TransactionAbortedException {
	HeapPage hp = (HeapPage) Database.getBufferPool().getPage(mTransactionId, mPageId, null);
	mTupleIterator = hp.iterator();
	mIsClosed = false;

    }

    public boolean hasNext() throws DbException, TransactionAbortedException, NoSuchElementException {
	if(mIsClosed)
	    return false;
	if(mPageNo < mMaxPages - 1 || mTupleIterator.hasNext()) {
	    return true;
	}
	else {
	    return false;
	}
    }

    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
	if(mIsClosed) throw new NoSuchElementException("Iterator is closed");
	
	if(this.hasNext()) {
	    if(mTupleIterator.hasNext())
		return mTupleIterator.next();
	    else {
		mPageNo++;
		mPageId = new HeapPageId(mHeapFile.getId(), mPageNo);
		HeapPage hp = (HeapPage) Database.getBufferPool().getPage(mTransactionId, mPageId, null);
		mTupleIterator = hp.iterator();
		return mTupleIterator.next();
	    }
	} else {
	    throw new NoSuchElementException("No more tuples left to iterate");
	}
    }

    public void rewind() throws DbException, TransactionAbortedException {
	if(mIsClosed) throw new IllegalStateException("Iterator is closed");

	mPageNo = 0;
	mPageId = new HeapPageId(mHeapFile.getId(), mPageNo);
	try {
	    HeapPage hp = (HeapPage) Database.getBufferPool().getPage(mTransactionId, mPageId, null);
	    mTupleIterator = hp.iterator();
	} catch (TransactionAbortedException e) {
	    //Cry
	    e.printStackTrace();
	    System.exit(1);
	} catch (DbException e) {
		e.printStackTrace();
		System.exit(2);
	}

    }
    
    public TupleDesc getTupleDesc() {
	return mHeapFile.getTupleDesc();
    }

    public void close() {
	mIsClosed = true;
    }
}
