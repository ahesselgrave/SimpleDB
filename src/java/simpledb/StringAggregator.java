package simpledb;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int mGroupByField;
    private Type mGroupByType;
    private int mAggregateField;
    private Op mOperator;
    private HashMap<Field, Integer> mCount;
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        if (what == Op.COUNT)
            mOperator = what;
        else throw new IllegalArgumentException("Operator for StringAggregator can only be Op.COUNT");
        mGroupByField = gbfield;
        mGroupByType = gbfieldtype;
        mAggregateField = afield;
        mCount = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field groupByField = mGroupByField == NO_GROUPING ? null :
                                                            tup.getField(mGroupByField);
        if (!mCount.containsKey(groupByField))
            mCount.put(groupByField, 1);
        else
            mCount.put(groupByField, mCount.get(groupByField) + 1);
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        ArrayList<Tuple> tupleArrayList = new ArrayList<>();

        // Create TupleDesc based on field index
        Type[] types;
        String[] names;
        if (mGroupByField == NO_GROUPING) {
            types = new Type[]{Type.INT_TYPE};
            names = new String[]{"aggregateValue"};
        } else {
            types = new Type[]{mGroupByType, Type.INT_TYPE};
            names = new String[]{"groupValue", "aggregateValue"};
        }
        TupleDesc td = new TupleDesc(types, names);

        // Iterate through all fields in the hashmap
        for (Field f : mCount.keySet()) {
            Tuple t = new Tuple(td);
            int aggregateValue = mCount.get(f);

            if (mAggregateField == NO_GROUPING) {
                t.setField(0, new IntField(aggregateValue));
            } else {
                t.setField(0, f);
                t.setField(1, new IntField(aggregateValue));
            }

            tupleArrayList.add(t);
        }
        return new TupleIterator(td, tupleArrayList);
    }

}
