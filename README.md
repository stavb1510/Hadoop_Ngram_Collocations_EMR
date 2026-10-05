# Collocation Extraction with Hadoop on AWS EMR

A multi-step **Hadoop MapReduce** pipeline that finds the most significant word pairs (collocations) in the **Google Books N-grams** corpus, per decade, in English and Hebrew. It runs on **AWS EMR** over the public N-grams dataset, which is hundreds of gigabytes.

Each bigram is scored with the **log-likelihood ratio (LLR)**, and the pipeline outputs the top 100 bigrams for every decade.

**Authors:** Stav Balaish ([@stavb1510](https://github.com/stavb1510)) · Kfir Bar Kochva ([@kfirbr2](https://github.com/kfirbr2))
Built for the Distributed Systems Programming course at Ben-Gurion University (2025).

---

## Pipeline

| Step | Input | Output |
|---|---|---|
| **1. Unigram counts** | 1-grams | `(decade, word) → count` and `(decade, *) → total` |
| **2. Join bigrams with unigram counts** | Step 1 + 2-grams | `(decade, w2) → (w1, c12, c1)` |
| **3. Score** | Step 2 + totals | LLR score per bigram per decade |
| **4. Top-100** | Step 3 | 100 highest-scoring bigrams per decade |

Step 1 filters out stopwords, words shorter than 3 characters, numbers and punctuation.

**The join in step 2** is done with secondary sort: a custom `Writable` key, `Partitioner`, `SortComparator` and `GroupingComparator`. Together they make each word's unigram count reach the reducer before all the bigrams that need it, so the join streams through the data instead of holding it in memory.

A **combiner** can be turned on or off from the command line, and we measured how much each option costs at runtime.

## Project structure

```
src/main/java/bgu/dsp/
├── aws/           EmrRunner — creates the cluster and submits all steps as one job flow
├── jobs/          job configuration for steps 1–4
├── steps/         mapper / reducer logic
├── writables/     custom Writable types
├── comparators/   sort and grouping comparators
├── partitioners/  custom partitioners
└── utils/         stopword lists and helpers
src/main/resources/  English and Hebrew stopwords
```

## Running

Requirements: JDK 8+, Maven, AWS credentials, an S3 bucket and an EC2 key pair (set in `EmrRunner`).

```bash
mvn clean package
java -jar target/assignment2-1.0-SNAPSHOT.jar true    # true = with combiner, false = without
```

Input comes from the public AWS dataset `s3://datasets.elasticmapreduce/ngrams/books/20090715/{eng-gb-all,heb-all}/{1gram,2gram}/data` (SequenceFile format).

## Tech

Java 8 · Hadoop MapReduce · AWS EMR · S3
