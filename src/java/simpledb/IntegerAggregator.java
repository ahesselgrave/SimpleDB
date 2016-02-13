package simpledb;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int mGroupByField;
    private Type mGroupByType;
    private int mAggregateField;
    private Op mOperator;

    /**
     * Used to map fields to their respective aggregation
     */
    private HashMap<Field, Integer> mAggregation;

    /**
     * Used as a helper for average
     */
    private HashMap<Field, Integer> mCount;


    /**
     * Aggregate constructor
     *
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */
    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        mGroupByField = gbfield;
        mGroupByType = gbfieldtype;
        mAggregateField = afield;
        mOperator = what;
        mAggregation = new HashMap<>();
        mCount = new HashMap<>();
    }

    private int initialValueBasedOnOp() {
        switch(mOperator) {
            case MIN:
                return Integer.MAX_VALUE;
            case MAX:
                return Integer.MIN_VALUE;
            case AVG:
            case SUM:
            case COUNT:
                return 0;
            default:
                throw new RuntimeException("you broke Java");
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field groupByField = (mGroupByField == Aggregator.NO_GROUPING) ? null : tup.getField(mGroupByField);

        if (!mAggregation.containsKey(groupByField)) {
            mAggregation.put(groupByField, initialValueBasedOnOp());
            mCount.put(groupByField, 0);
        }

        int tupVal = ((IntField) tup.getField(mAggregateField)).getValue();
        int currentAggregationVal = mAggregation.get(groupByField);
        int currentCountVal = mCount.get(groupByField);

        int newAggregationVal = currentAggregationVal;

        switch(mOperator) {
            case MIN:
                newAggregationVal = (tupVal < currentAggregationVal) ? tupVal : currentAggregationVal;
                break;
            case MAX:
                newAggregationVal = (tupVal > currentAggregationVal) ? tupVal : currentAggregationVal;
                break;
            case SUM:
            case AVG:
                // Average can't be calculated until all tuples are merged, so we just
                // treat it as a sum and divide by count when we return the iterator
                newAggregationVal = tupVal + currentAggregationVal;
                mCount.put(groupByField, currentCountVal + 1);
                break;
            case COUNT:
                newAggregationVal = currentAggregationVal + 1;
                break;
            default:
                break;
        }
        mAggregation.put(groupByField, newAggregationVal);
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
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


        // Iterate through all fields in the aggregate hashmap
        for (Field f : mAggregation.keySet()) {
            Tuple t = new Tuple(td);

            // Check the average case now
            int aggregateValue = (mOperator == Op.AVG) ? mAggregation.get(f) / mCount.get(f) :
                                                         mAggregation.get(f);

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
