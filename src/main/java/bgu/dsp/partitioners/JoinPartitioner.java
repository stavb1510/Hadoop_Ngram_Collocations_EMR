package bgu.dsp.partitioners;

import bgu.dsp.writables.JoinKeyWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Partitions by (decade, joinWord) only.
 * Ignores tagOrder to ensure UNIGRAM and BIGRAM/PARTIAL for the same join-group
 * go to the same reducer.
 */
public class JoinPartitioner extends Partitioner<JoinKeyWritable, Writable> {

    @Override
    public int getPartition(JoinKeyWritable key, Writable value, int numPartitions) {
        // Combine decade + joinWord hash (ignore tagOrder)
        int h = 163; // small seed
        h = 31 * h + Integer.hashCode(key.getDecade());
        h = 31 * h + key.getJoinWord().hashCode();

        // Ensure non-negative partition index
        return (h & Integer.MAX_VALUE) % numPartitions;
    }
}