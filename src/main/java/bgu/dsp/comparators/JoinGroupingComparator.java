package bgu.dsp.comparators;

import bgu.dsp.writables.JoinKeyWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Groups keys for the reducer by (decade, joinWord) only.
 * Ignores tagOrder so that UNIGRAM + BIGRAM/PARTIAL are processed in one reduce() call.
 */
public class JoinGroupingComparator extends WritableComparator {

    public JoinGroupingComparator() {
        super(JoinKeyWritable.class, true);
    }
    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        JoinKeyWritable k1 = (JoinKeyWritable) a;
        JoinKeyWritable k2 = (JoinKeyWritable) b;

        int cmp = Integer.compare(k1.getDecade(), k2.getDecade());
        if (cmp != 0) return cmp;

        return k1.getJoinWord().compareTo(k2.getJoinWord());
    }
}