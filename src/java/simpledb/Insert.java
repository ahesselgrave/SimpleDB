package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private TupleDesc mTupleDesc;
    private TransactionId mTransactionId;
    private DbIterator mDbIterator;
    private int mTableId;

    private boolean fetchNextCalled = false;

    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, DbIterator child, int tableid)
            throws DbException {
        mTransactionId = t;
        mDbIterator = child;
        mTableId = tableid;
        mTupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"numInsertedTuples"});

        // Check the tableid TupleDesc
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableid);
        assert(child.getTupleDesc().equals(dbFile.getTupleDesc()));
    }

    public TupleDesc getTupleDesc() {
        return mTupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        mDbIterator.open();
    }

    public void close() {
        super.close();
        mDbIterator.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        mDbIterator.rewind();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (!fetchNextCalled) {
            int insertCount = 0;
            while (mDbIterator.hasNext()) {
                Tuple t = mDbIterator.next();
                try {
                    Database.getBufferPool().insertTuple(mTransactionId, mTableId, t);
                    insertCount++;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            Tuple retTuple = new Tuple(mTupleDesc);
            retTuple.setField(0, new IntField(insertCount));
            fetchNextCalled = true;
            return retTuple;
        } else
            return null;
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[]{mDbIterator};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        mDbIterator = children[0];
    }
}
