package com.example.job;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.springframework.stereotype.Component;

@Component
public class WordCountJob {

    // Flink's execution environment is provided by our Spring configuration.
    // Keeping it injected makes the job easier to test and easier to evolve.
    private final StreamExecutionEnvironment env;

    public WordCountJob(StreamExecutionEnvironment env) {
        this.env = env;
    }

    public void execute() throws Exception {
        // This is the input stream for our demo. Each String record represents
        // one line of text that will be broken into words.
        DataStream<String> input = env.fromElements(
                "hello world",
                "hello flink",
                "apache flink with spring boot",
                "hello spring boot");

        DataStream<Integer> stream2 = env.fromElements(1, 2);

        input.connect(stream2).map(new CoMapJoin()).print();


        env.fromElements("hello world", "flink is fast")
                .flatMap(new WordSplitter())
                .print();

        env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
                .broadcast()
                .filter(x -> x % 2 == 0)
                .print();

        env.fromElements(Tuple2.of("A", 1), Tuple2.of("A", 2), Tuple2.of("C", 3), Tuple2.of("C", 4))
                .keyBy(value -> value.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(2)))
                .reduce((a, b) -> Tuple2.of(a.f0, a.f1 + b.f1))
                .print();


        // The classic Flink word count pipeline:
        // 1. flatMap: split every input line into individual words
        // 2. keyBy: group all identical words together
        // 3. sum(1): keep a running count in tuple field index 1
        DataStream<Tuple2<String, Integer>> counts = input
                .flatMap(new Tokenizer())
                .keyBy(value -> value.f0)
                .sum(1);


        // print() is the simplest local sink for demos and debugging.
        // For a streaming aggregation, this prints every updated count,
        // not just the final total once per word.
        counts.print("[WordCount]");

        // Trigger the Flink job. Without execute(), the pipeline is only
        // defined but never actually runs.
        env.execute("word-count-job");
    }

    private static class CoMapJoin implements CoMapFunction<String, Integer, String> {
        @Override
        public String map1(String value) throws Exception {
            return "Stream 1: " + value;
        }

        @Override
        public String map2(Integer value) throws Exception {
            return "Stream 2: " + value;
        }
    }

    private static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> out) {
            // Normalize the line to lowercase so HELLO and hello are counted
            // as the same logical word.
            for (String word : line.toLowerCase().split("\\W+")) {
                // Ignore empty tokens that can appear when the split hits
                // multiple separators or punctuation-only segments.
                if (!word.isBlank()) {
                    // Emit (word, 1) for every word occurrence.
                    // The later keyBy + sum step turns these individual
                    // occurrences into aggregated word counts.
                    out.collect(Tuple2.of(word, 1));
                }
            }
        }
    }

    private static class WordSplitter implements FlatMapFunction<String, String> {
        @Override
        public void flatMap(String line, Collector<String> out) {
            for (String word : line.split(" ")) {
                out.collect(word);
            }
        }
    }
}
