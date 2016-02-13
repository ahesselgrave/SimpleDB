package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private TupleDesc mTupleDesc;
    private TransactionId mTransactionId;
    private DbIterator mIterator;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        mTransactionId = t;
        mIterator = child;
        mTupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"numDeletedTuples"});
    }

    public TupleDesc getTupleDesc() {
        return mTupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        mIterator.open();
    }

    public void close() {
        super.close();
        mIterator.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        mIterator.rewind();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     *
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        int deleteCount = 0;
        while (mIterator.hasNext()) {
            Tuple t = mIterator.next();
            try {
                Database.getBufferPool().deleteTuple(mTransactionId, t);
                deleteCount++;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        Tuple retTuple = new Tuple(mTupleDesc);
        retTuple.setField(0, new IntField(deleteCount));
        return retTuple;
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[]{mIterator};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        mIterator = children[0];
    }

}
