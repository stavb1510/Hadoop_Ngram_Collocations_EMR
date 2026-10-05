package bgu.dsp.writables;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * Composite key for reduce-side join + secondary sort.
 *
 * Grouping key (handled by JoinGroupingComparator): (decade, joinWord)
 * Sorting key (compareTo): (decade, joinWord, tagOrder)
 *
 * tagOrder is used to force the UNIGRAM record to arrive first in the reducer.
 */
public class JoinKeyWritable implements WritableComparable<JoinKeyWritable> {

    /**
     * Tag ordering:
     * smaller tagOrder => arrives earlier in reducer iteration.
     */
    public static final byte TAG_UNIGRAM = 0;  // C1/C2 record
    public static final byte TAG_BIGRAM  = 1;  // bigram record (Step2)
    public static final byte TAG_PARTIAL = 1;  // partial record (Step3) - same priority as bigram is fine

    private int decade;        // e.g., 1990, 2000
    private Text joinWord;     // w1 in Step2, w2 in Step3
    private byte tagOrder;     // ensures UNIGRAM first

    // Hadoop needs a no-arg constructor
    public JoinKeyWritable() {
        this.decade = 0;
        this.joinWord = new Text();
        this.tagOrder = TAG_UNIGRAM;
    }

    public JoinKeyWritable(int decade, String joinWord, byte tagOrder) {
        this.decade = decade;
        this.joinWord = new Text(joinWord);
        this.tagOrder = tagOrder;
    }

    public JoinKeyWritable(int decade, Text joinWord, byte tagOrder) {
        this.decade = decade;
        this.joinWord = new Text(joinWord); // defensive copy
        this.tagOrder = tagOrder;
    }

    public int getDecade() {
        return decade;
    }

    public Text getJoinWord() {
        return joinWord;
    }

    public byte getTagOrder() {
        return tagOrder;
    }

    public void set(int decade, String joinWord, byte tagOrder) {
        this.decade = decade;
        this.joinWord.set(joinWord);
        this.tagOrder = tagOrder;
    }

    public void setDecade(int decade) {
        this.decade = decade;
    }

    public void setJoinWord(String joinWord) {
        this.joinWord.set(joinWord);
    }

    public void setJoinWord(Text joinWord) {
        this.joinWord.set(joinWord);
    }

    public void setTagOrder(byte tagOrder) {
        this.tagOrder = tagOrder;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(decade);
        joinWord.write(out);
        out.writeByte(tagOrder);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        decade = in.readInt();
        joinWord.readFields(in);
        tagOrder = in.readByte();
    }

    /**
     * Sort order for secondary sort:
     * (decade asc, joinWord lex asc, tagOrder asc)
     */
    @Override
    public int compareTo(JoinKeyWritable other) {
        int cmp = Integer.compare(this.decade, other.decade);
        if (cmp != 0) return cmp;

        cmp = this.joinWord.compareTo(other.joinWord);
        if (cmp != 0) return cmp;

        return Byte.compare(this.tagOrder, other.tagOrder);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof JoinKeyWritable)) return false;
        JoinKeyWritable that = (JoinKeyWritable) o;
        return decade == that.decade &&
               tagOrder == that.tagOrder &&
               Objects.equals(joinWord, that.joinWord);
    }

    @Override
    public int hashCode() {
        // Note: partitioner/grouping will ignore tagOrder, but hashCode is still fine as full key identity.
        return Objects.hash(decade, joinWord, tagOrder);
    }

    @Override
    public String toString() {
        return decade + "\t" + joinWord.toString() + "\t" + tagOrder;
    }
}