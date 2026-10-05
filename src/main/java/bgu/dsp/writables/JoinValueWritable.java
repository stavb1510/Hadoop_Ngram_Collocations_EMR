package bgu.dsp.writables;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Tagged value for reduce-side joins.
 *
 * Used in:
 *  - Step2: JoinC1 (UNIGRAM_C1 + BIGRAM)
 *  - Step3: JoinC2 + LLR (UNIGRAM_C2 + PARTIAL)
 *
 * Tags decide which fields are present.
 */
public class JoinValueWritable implements Writable {

    // --- Tags (what kind of record is this?) ---
    public static final byte TAG_C1      = 0; // unigram count for w1 (Step2)
    public static final byte TAG_BIGRAM  = 1; // bigram (w1,w2,c12) grouped by w1 (Step2)
    public static final byte TAG_C2      = 2; // unigram count for w2 (Step3)
    public static final byte TAG_PARTIAL = 3; // output of Step2: (w1,w2,c12,c1) grouped by w2 (Step3)

    private byte tag;

    // We'll reuse these Text objects to avoid allocations
    private final Text w1 = new Text();
    private final Text w2 = new Text();

    // counts:
    // - for TAG_C1 / TAG_C2: count holds c1 or c2
    // - for TAG_BIGRAM: c12 holds bigram count
    // - for TAG_PARTIAL: c1 and c12 are used
    private long count; // c1 or c2 (depending on tag)
    private long c1;    // for PARTIAL
    private long c12;   // for BIGRAM / PARTIAL

    // Hadoop needs a no-arg constructor
    public JoinValueWritable() {
        this.tag = TAG_C1;
        this.count = 0;
        this.c1 = 0;
        this.c12 = 0;
    }

    // ----------------- Builders / Setters -----------------

    /** UNIGRAM record: c1 (Step2) */
    public void setC1(long c1Count) {
        this.tag = TAG_C1;
        this.count = c1Count;
        this.w1.clear();
        this.w2.clear();
        this.c1 = 0;
        this.c12 = 0;
    }

    /** UNIGRAM record: c2 (Step3) */
    public void setC2(long c2Count) {
        this.tag = TAG_C2;
        this.count = c2Count;
        this.w1.clear();
        this.w2.clear();
        this.c1 = 0;
        this.c12 = 0;
    }

    /** BIGRAM record (for Step2 reducer): carries w2 + c12 (w1 is in the key) */
    public void setBigram(String w2Str, long c12Count) {
        this.tag = TAG_BIGRAM;
        this.w2.set(w2Str);
        this.c12 = c12Count;

        this.w1.clear();
        this.count = 0;
        this.c1 = 0;
    }

    /** PARTIAL record (output of Step2, input to Step3 reducer): carries w1 + w2 + c12 + c1 */
    public void setPartial(String w1Str, String w2Str, long c12Count, long c1Count) {
        this.tag = TAG_PARTIAL;
        this.w1.set(w1Str);
        this.w2.set(w2Str);
        this.c12 = c12Count;
        this.c1 = c1Count;

        this.count = 0;
    }

    // ----------------- Getters -----------------

    public byte getTag() {
        return tag;
    }

    /** For TAG_C1/TAG_C2 only */
    public long getCount() {
        return count;
    }

    /** For TAG_BIGRAM: w2 ; For TAG_PARTIAL: w2 */
    public Text getW2() {
        return w2;
    }

    /** For TAG_PARTIAL only */
    public Text getW1() {
        return w1;
    }

    /** For TAG_BIGRAM/TAG_PARTIAL */
    public long getC12() {
        return c12;
    }

    /** For TAG_PARTIAL */
    public long getC1() {
        return c1;
    }

    // ----------------- Writable -----------------

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeByte(tag);

        switch (tag) {
            case TAG_C1:
            case TAG_C2:
                out.writeLong(count);
                break;

            case TAG_BIGRAM:
                w2.write(out);
                out.writeLong(c12);
                break;

            case TAG_PARTIAL:
                w1.write(out);
                w2.write(out);
                out.writeLong(c12);
                out.writeLong(c1);
                break;

            default:
                throw new IOException("Unknown JoinValueWritable tag: " + tag);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        tag = in.readByte();

        // Clear old state (important when objects are reused)
        w1.clear();
        w2.clear();
        count = 0;
        c1 = 0;
        c12 = 0;

        switch (tag) {
            case TAG_C1:
            case TAG_C2:
                count = in.readLong();
                break;

            case TAG_BIGRAM:
                w2.readFields(in);
                c12 = in.readLong();
                break;

            case TAG_PARTIAL:
                w1.readFields(in);
                w2.readFields(in);
                c12 = in.readLong();
                c1 = in.readLong();
                break;

            default:
                throw new IOException("Unknown JoinValueWritable tag: " + tag);
        }
    }

    @Override
    public String toString() {
        switch (tag) {
            case TAG_C1:     return "C1:" + count;
            case TAG_C2:     return "C2:" + count;
            case TAG_BIGRAM: return "BIGRAM:w2=" + w2 + ",c12=" + c12;
            case TAG_PARTIAL:return "PARTIAL:w1=" + w1 + ",w2=" + w2 + ",c12=" + c12 + ",c1=" + c1;
            default:         return "UNKNOWN";
        }
    }
}