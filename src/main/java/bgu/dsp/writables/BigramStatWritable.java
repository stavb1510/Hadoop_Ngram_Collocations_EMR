package bgu.dsp.writables;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Represents a scored bigram: (w1, w2, llrScore).
 * Used as:
 *  - Output of Step3 (JoinC2 + LLR)
 *  - Input to Step4 (Top-100 per decade)
 */
public class BigramStatWritable implements Writable {

    private final Text w1 = new Text();
    private final Text w2 = new Text();
    private double llr;

    // Hadoop requires a no-arg constructor
    public BigramStatWritable() {
        this.llr = 0.0;
    }

    public BigramStatWritable(String w1, String w2, double llr) {
        this.w1.set(w1);
        this.w2.set(w2);
        this.llr = llr;
    }

    // -------- setters --------
    public void set(String w1, String w2, double llr) {
        this.w1.set(w1);
        this.w2.set(w2);
        this.llr = llr;
    }

    // -------- getters --------
    public Text getW1() {
        return w1;
    }

    public Text getW2() {
        return w2;
    }

    public double getLlr() {
        return llr;
    }

    // -------- Writable --------
    @Override
    public void write(DataOutput out) throws IOException {
        w1.write(out);
        w2.write(out);
        out.writeDouble(llr);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        w1.readFields(in);
        w2.readFields(in);
        llr = in.readDouble();
    }

    @Override
    public String toString() {
        return w1.toString() + "\t" + w2.toString() + "\t" + llr;
    }
}