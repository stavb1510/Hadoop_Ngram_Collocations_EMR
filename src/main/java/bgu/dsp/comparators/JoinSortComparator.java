package bgu.dsp.comparators;

import bgu.dsp.writables.JoinKeyWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Sorts keys by (decade, joinWord, tagOrder).
 * Ensures that for a given (decade, joinWord) group,
 * the UNIGRAM record (tag=0) comes before BIGRAM records (tag=1).
 */
public class JoinSortComparator extends WritableComparator {

    public JoinSortComparator() {
        super(JoinKeyWritable.class, true);
    }
    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        JoinKeyWritable k1 = (JoinKeyWritable) a;
        JoinKeyWritable k2 = (JoinKeyWritable) b;

        return k1.compareTo(k2);
    }
}