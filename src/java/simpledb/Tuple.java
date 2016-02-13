package simpledb;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {
    public static final int MAXSTRINGSIZE = 30;
    
    private static final long serialVersionUID = 1L;

    private TupleDesc mSchema;
    private RecordId mRecordId;

    private Field[] mFields;
    
    /**
     * Create a new tuple with the specified schema (type).
     * 
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        mSchema = td;
	    setFieldsAccordingToSchema();
    }

    public void setFieldsAccordingToSchema() {
        mFields = new Field[mSchema.numFields()];
        for (int i = 0; i < mSchema.numFields(); i++) {
            switch (mSchema.getFieldType(i)) {
            case INT_TYPE:
            mFields[i] = new IntField(0);
            break;
            case STRING_TYPE:
            mFields[i] = new StringField("", MAXSTRINGSIZE);
            break;
            default:
            // cry
            break;
            }
        }
    }
    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
	return mSchema;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
	return mRecordId;
    }

    /**
     * Set the RecordId information for this tuple.
     * 
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
	mRecordId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     * 
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
	mFields[i] = f;
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     * 
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        return mFields[i];
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * 
     * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
     * 
     * where \t is any whitespace, except newline, and \n is a newline
     */
    public String toString() {
	String output = mFields[0].toString();
	for (int i = 1; i < mFields.length; i++) {
	    output += String.format(" %s", mFields[i].toString());
	}
	output += "\n";
	return output;
    }
    
    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
	Iterator<Field> iter = new Iterator<Field>() {
	    private int index = 0;

	    @Override
	    public boolean hasNext() {
		return index < mFields.length;
	    }

	    @Override
	    public Field next() {
		return mFields[index++];
	    }
	    
	    @Override
	    public void remove() {
		throw new UnsupportedOperationException();
	    }
	};
        return iter;
    }
    
    /**
     * reset the TupleDesc of this tuple
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        mSchema = td;
	setFieldsAccordingToSchema();
    }
}
