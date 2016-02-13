package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */         //hOI im tem
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }
    
    /**
     * The actual container for the TDItems
     */
    private TDItem[] mTDItems;
    
    /**
     * Size of the TDITem list for the iterator
     */
    private int mSize;
    
    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        Iterator<TDItem> iter = new Iterator<TDItem>() {
	    private int index = 0;

	    @Override
	    public boolean hasNext() {
		return index < mSize;
	    }

	    @Override
	    public TDItem next() {
		return mTDItems[index++];
	    }

	    @Override
	    public void remove() {
		throw new UnsupportedOperationException();
	    }
	};
	return iter;
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
	int typeArSize  = typeAr.length,
	    fieldArSize = fieldAr.length;
	if ( (typeArSize == 0 || fieldArSize == 0) ||
	     (typeArSize != fieldArSize) ) {
	    throw new IllegalArgumentException("typeAr and fieldAr must have the same length!");
	} else {
	    mTDItems = new TDItem[typeArSize];
	    for (int i = 0; i < typeArSize; i++) {
		mTDItems[i] = new TDItem(typeAr[i], fieldAr[i]);
	    }
	}
	mSize = mTDItems.length;
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
	mTDItems = new TDItem[typeAr.length];
	for (int i = 0; i < typeAr.length; i++) {
	    mTDItems[i] = new TDItem(typeAr[i], "");
	}
	mSize = mTDItems.length;
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
	return mSize;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
	if (i >= mSize)
	    throw new NoSuchElementException(String.format("%d is over the %d size", i, mSize));
	else 
	    return mTDItems[i].fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
	if (i >= mSize) 
	    throw new NoSuchElementException(String.format("%d is over the %d size", i, mSize));
	else
	    return mTDItems[i].fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        for (int i = 0; i < mSize; i++) {
	    if (mTDItems[i].fieldName.equals(name))
		return i;
	}
	throw new NoSuchElementException(String.format("No field with name %s exists!",name));
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
	int accum = 0;
	for (int i = 0; i < mSize; i++) {
	    accum += mTDItems[i].fieldType.getLen();
	}
	return accum;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
	int td1Size = td1.numFields(), td2Size = td2.numFields();

	// this code block runs 3 times as fast as regular code blocks
	Type[] newTypes = new Type[td1Size + td2Size];
	String[] newFields = new String[td1Size + td2Size];
	
	for (int i = 0; i < td1Size; i++) { 
	    newTypes[i] = td1.getFieldType(i);
	    newFields[i] = td1.getFieldName(i);
	}
	for (int i = 0; i < td2Size; i++) {
	    newTypes [i + td1Size] = td2.getFieldType(i);
	    newFields[i + td1Size] = td2.getFieldName(i);
	}
	return new TupleDesc(newTypes, newFields);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
	if (o instanceof TupleDesc) {
	    TupleDesc tdo = (TupleDesc) o;
	    if (tdo.numFields() != mSize) return false;
	    
	    for (int i = 0; i < mSize; i++) {
			if (!tdo.getFieldType(i).equals(this.getFieldType(i)))
				return false;
	    }
	    return true;
	}
	else return false;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
	String output = "";
        for (int i = 0; i < mSize; i++) {
	    output += String.format("%s(%s),", getFieldType(i).toString(), getFieldName(i));
	}
	return output;
    }
}
