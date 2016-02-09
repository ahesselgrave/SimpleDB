package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    private Predicate mPredicate;
    private DbIterator mChild;

    public Filter(Predicate p, DbIterator child) {
	mPredicate = p;
	mChild = child;
    }

    public Predicate getPredicate() {
	return mPredicate;
    }

    public TupleDesc getTupleDesc() {
        return mChild.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
	super.open();
        mChild.open();
    }

    public void close() {
	super.close();
	mChild.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
	mChild.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
	while (mChild.hasNext()) {
	    Tuple next = mChild.next();
	    if (mPredicate.filter(next))
		return next;
	}
	return null;
    }

    @Override
    public DbIterator[] getChildren() {
	return new DbIterator[]{mChild};
    }

    @Override
    public void setChildren(DbIterator[] children) {
	mChild = children[0];
    }

}
