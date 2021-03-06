package simpledb;

import java.util.*;
import java.io.*;

/**
 * Each instance of HeapPage stores data for one page of HeapFiles and
 * implements the Page interface that is used by BufferPool.
 * 
 * @see HeapFile
 * @see BufferPool
 * 
 */
public class HeapPage implements Page {

	final HeapPageId mHeapPageId;
	final TupleDesc mTupleDesc;
	byte mHeader[];
	Tuple mTuples[];
	int mNumSlots;
	TransactionId mTransactionId;

	byte[] oldData;
	private final Byte oldDataLock = new Byte((byte) 0);

	/**
	 * Create a HeapPage from a set of bytes of data read from disk. The format
	 * of a HeapPage is a set of mHeader bytes indicating the slots of the page
	 * that are in use, some number of tuple slots. Specifically, the number of
	 * mTuples is equal to:
	 * <p>
	 * floor((BufferPool.getPageSize()*8) / (tuple size * 8 + 1))
	 * <p>
	 * where tuple size is the size of mTuples in this database table, which can
	 * be determined via {@link Catalog#getTupleDesc}. The number of 8-bit
	 * mHeader words is equal to:
	 * <p>
	 * ceiling(no. tuple slots / 8)
	 * <p>
	 * 
	 * @see Database#getCatalog
	 * @see Catalog#getTupleDesc
	 * @see BufferPool#getPageSize()
	 */
	public HeapPage(HeapPageId id, byte[] data) throws IOException {
		this.mHeapPageId = id;
		this.mTupleDesc = Database.getCatalog().getTupleDesc(id.getTableId());

		this.mNumSlots = getNumTuples();
		DataInputStream dis = new DataInputStream(
				new ByteArrayInputStream(data));

		// allocate and read the mHeader slots of this page
		mHeader = new byte[getHeaderSize()];
		for (int i = 0; i < mHeader.length; i++)
			mHeader[i] = dis.readByte();

		mTuples = new Tuple[mNumSlots];
		try {
			// allocate and read the actual records of this page
			for (int i = 0; i < mTuples.length; i++)
				mTuples[i] = readNextTuple(dis, i);
		} catch (NoSuchElementException e) {
			e.printStackTrace();
		}
		dis.close();

		setBeforeImage();
	}

	/**
	 * Retrieve the number of mTuples on this page.
	 * 
	 * @return the number of mTuples on this page
	 */
	private int getNumTuples() {
		// some code goes here
		int bitsPerTupleIncludingHeader = mTupleDesc.getSize() * 8 + 1;
		int tuplesPerPage = (BufferPool.getPageSize() * 8)
				/ bitsPerTupleIncludingHeader; // round down
		return tuplesPerPage;
	}

	/**
	 * Computes the number of bytes in the mHeader of a page in a HeapFile with
	 * each tuple occupying tupleSize bytes
	 * 
	 * @return the number of bytes in the mHeader of a page in a HeapFile with
	 *         each tuple occupying tupleSize bytes
	 */
	private int getHeaderSize() {
		// some code goes here
		int tuplesPerPage = getNumTuples();
		int headerBytes = (tuplesPerPage / 8);
		if (headerBytes * 8 < tuplesPerPage) {
			headerBytes++;
		}

		return headerBytes;
	}

	/**
	 * Return a view of this page before it was modified -- used by recovery
	 */
	public HeapPage getBeforeImage() {
		try {
			byte[] oldDataRef = null;
			synchronized (oldDataLock) {
				oldDataRef = oldData;
			}
			return new HeapPage(mHeapPageId, oldDataRef);
		} catch (IOException e) {
			e.printStackTrace();
			// should never happen -- we parsed it OK before!
			System.exit(1);
		}
		return null;
	}

	public void setBeforeImage() {
		synchronized (oldDataLock) {
			oldData = getPageData().clone();
		}
	}

	/**
	 * @return the PageId associated with this page.
	 */
	public HeapPageId getId() {
		// some code goes here
		return mHeapPageId;
	}

	/**
	 * Suck up mTuples from the source file.
	 */
	private Tuple readNextTuple(DataInputStream dis, int slotId)
			throws NoSuchElementException {

		// if associated bit is not set, read forward to the next tuple, and
		// return null.
		if (!isSlotUsed(slotId)) {
			for (int i = 0; i < mTupleDesc.getSize(); i++) {
				try {
					dis.readByte();
				} catch (IOException e) {
					throw new NoSuchElementException(
							"error reading empty tuple");
				}
			}
			return null;
		}

		// read fields in the tuple
		Tuple t = new Tuple(mTupleDesc);
		RecordId rid = new RecordId(mHeapPageId, slotId);
		t.setRecordId(rid);
		try {
			for (int j = 0; j < mTupleDesc.numFields(); j++) {
				Field f = mTupleDesc.getFieldType(j).parse(dis);
				t.setField(j, f);
			}
		} catch (java.text.ParseException e) {
			e.printStackTrace();
			throw new NoSuchElementException("parsing error!");
		}

		return t;
	}

	/**
	 * Generates a byte array representing the contents of this page. Used to
	 * serialize this page to disk.
	 * <p>
	 * The invariant here is that it should be possible to pass the byte array
	 * generated by getPageData to the HeapPage constructor and have it produce
	 * an identical HeapPage object.
	 * 
	 * @see #HeapPage
	 * @return A byte array correspond to the bytes of this page.
	 */
	public byte[] getPageData() {
		int len = BufferPool.getPageSize();
		ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
		DataOutputStream dos = new DataOutputStream(baos);

		// create the mHeader of the page
		for (int i = 0; i < mHeader.length; i++) {
			try {
				dos.writeByte(mHeader[i]);
			} catch (IOException e) {
				// this really shouldn't happen
				e.printStackTrace();
			}
		}

		// create the mTuples
		for (int i = 0; i < mTuples.length; i++) {

			// empty slot
			if (!isSlotUsed(i)) {
				for (int j = 0; j < mTupleDesc.getSize(); j++) {
					try {
						dos.writeByte(0);
					} catch (IOException e) {
						e.printStackTrace();
					}

				}
				continue;
			}

			// non-empty slot
			for (int j = 0; j < mTupleDesc.numFields(); j++) {
				Field f = mTuples[i].getField(j);
				try {
					f.serialize(dos);

				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		// padding
		int zerolen = BufferPool.getPageSize()
				- (mHeader.length + mTupleDesc.getSize() * mTuples.length); // -
																		// mNumSlots
																		// *
																		// td.getSize();
		byte[] zeroes = new byte[zerolen];
		try {
			dos.write(zeroes, 0, zerolen);
		} catch (IOException e) {
			e.printStackTrace();
		}

		try {
			dos.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return baos.toByteArray();
	}

	/**
	 * Static method to generate a byte array corresponding to an empty
	 * HeapPage. Used to add new, empty pages to the file. Passing the results
	 * of this method to the HeapPage constructor will create a HeapPage with no
	 * valid mTuples in it.
	 * 
	 * @return The returned ByteArray.
	 */
	public static byte[] createEmptyPageData() {
		int len = BufferPool.getPageSize();
		return new byte[len]; // all 0
	}

	/**
	 * Delete the specified tuple from the page; the tuple should be updated to
	 * reflect that it is no longer stored on any page.
	 * 
	 * @throws DbException
	 *             if this tuple is not on this page, or tuple slot is already
	 *             empty.
	 * @param t
	 *            The tuple to delete
	 */
	public void deleteTuple(Tuple t) throws DbException {
		// some code goes here
		// not necessary for lab1

		// check rid to see if on page
		if (t.getRecordId()!=null && t.getRecordId().getPageId().equals(mHeapPageId)) {
			int tupleIndex = t.getRecordId().tupleno();
			if (isSlotUsed(tupleIndex)) {
				markSlotUsed(tupleIndex, false);
				t.setRecordId(null);
				mTuples[tupleIndex] = null;
				return;
			}
		} 
			throw new DbException(
					"tuple not in this page or is already deleted");
	}

	/**
	 * Adds the specified tuple to the page; the tuple should be updated to
	 * reflect that it is now stored on this page.
	 * 
	 * @throws DbException
	 *             if the page is full (no empty slots) or tupledesc is
	 *             mismatch.
	 * @param t
	 *            The tuple to add.
	 */
	public void insertTuple(Tuple t) throws DbException {
		// some code goes here
		// not necessary for lab1
		if (getNumEmptySlots() == 0)
			throw new DbException("page is full");
		else if (!mTupleDesc.equals(t.getTupleDesc()))
			throw new DbException("Tuple Desc mismatch");
		else {
			// get the first empty slot
			int i;
			for (i = 0; i < mNumSlots; i++) {
				if (!isSlotUsed(i))
					break;
			}
			t.setRecordId(new RecordId(mHeapPageId, i));//update the rid
			mTuples[i] = t;
			markSlotUsed(i, true);
		}
	}

	/**
	 * Marks this page as dirty/not dirty and record that transaction that did
	 * the dirtying
	 */
	public void markDirty(boolean dirty, TransactionId tid) {
		// some code goes here
		// not necessary for lab1
		mTransactionId =null;//default is not dirty
		if(dirty==true)
			mTransactionId =tid;
	}

	/**
	 * @return the tid of the transaction that last dirtied this page, or null
	 *         if the page is not dirty
	 */
	public TransactionId isDirty() {
		// some code goes here
		// Not necessary for lab1		
		return mTransactionId;
	}

	/**
	 * @return the number of empty slots on this page.
	 */
	public int getNumEmptySlots() {
		// some code goes here
		int count = 0;
		for (int i = 0; i < mNumSlots; i++) {
			if (!isSlotUsed(i)) {
				count++;
			}
		}
		return count;

	}

	/**
	 * @return true if associated slot on this page is filled, false otherwise.
	 */
	public boolean isSlotUsed(int i) {
		// some code goes here
		int headerBit = i % 8; // remainder is offset into byte
		int headerByte = (i - headerBit) / 8; // difference is the byte number
		return (mHeader[headerByte] & (1 << headerBit)) != 0;
	}

	/**
	 * Abstraction to fill or clear a slot on this page. 
	 */
	private void markSlotUsed(int i, boolean value) {
		// some code goes here
		// not necessary for lab1		
		int byteIndex=i/8;
        int bitIndex=i%8;
        if(value==true){//set bit to 1, which means used
        	mHeader[byteIndex]=(byte)(mHeader[byteIndex]|1<<bitIndex);
        } else {//set bit to 0, which mean unused
        	mHeader[byteIndex]=(byte)(mHeader[byteIndex]& ~(1<<bitIndex));
        }
	}

	/**
	 * @return an iterator over all mTuples on this page (calling remove on this
	 *         iterator throws an UnsupportedOperationException) (note that this
	 *         iterator shouldn't return mTuples in empty slots!)
	 */
	public Iterator<Tuple> iterator() {
		// some code goes here
		return new HeapPageIterator(this);
	}

	/**
	 * protected method used by the iterator to get the ith tuple out of this
	 * page
	 * 
	 * @param i
	 *            The index of the tuple to get.
	 * @throws NoSuchElementException
	 *             If the tuple with index i does not exist.
	 */
	protected Tuple getTuple(int i) throws NoSuchElementException {

		try {
			if (!isSlotUsed(i)) {
				return null;
			} else {
				return mTuples[i];
			}
		} catch (ArrayIndexOutOfBoundsException e) {
			throw new NoSuchElementException();
		}
	}

	/**
	 * Helper class that implements the Java Iterator for mTuples on a HeapPage.
	 */
	class HeapPageIterator implements Iterator<Tuple> {

		int mCurrentIndex = 0;
		Tuple mNext;
		HeapPage mHeapPage;

		/**
		 * Constructor sets the HeapPage for this iterator
		 * 
		 * @param p
		 *            The HeapPage to iterate over
		 */
		public HeapPageIterator(HeapPage p) {
			mHeapPage = p;
		}

		/**
		 * @return true if this iterator has another tuple, false otherwise.
		 */
		public boolean hasNext() {
			if (mNext != null) {
				return true;
			}
			try {
				while (true) {
					mNext = mHeapPage.getTuple(mCurrentIndex++);
					if (mNext != null)
						return true;
				}
			} catch (NoSuchElementException e) {
				return false;
			}
		}

		/**
		 * @return The next tuple.
		 * @throws NoSuchElementException
		 *             If no next tuple exists.
		 */
		public Tuple next() {
			Tuple next = mNext;

			if (next == null) {
				if (hasNext()) {
					next = mNext;
					mNext = null;
					return next;
				} else {
					throw new NoSuchElementException();
				}
			} else {
				mNext = null;
				return next;
			}
		}

		public void remove() {
			throw new UnsupportedOperationException();
		}
	}

}