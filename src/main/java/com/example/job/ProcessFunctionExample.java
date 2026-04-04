package com.example.job;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.springframework.stereotype.Component;

@Component
public class ProcessFunctionExample {

    // Side output lets us route special records out of the main stream without
    // breaking the primary business pipeline.
    private static final OutputTag<String> LONG_WORDS_TAG = new OutputTag<>("long-words") {
    };

    private final StreamExecutionEnvironment env;

    public ProcessFunctionExample(StreamExecutionEnvironment env) {
        this.env = env;
    }

    public void explainExample() throws Exception {
        // Build a small demo stream. In a real application this could come from
        // Kafka, a file source, or a REST-triggered ingestion pipeline.
        DataStream<String> words = env.fromElements(
                "Spring",
                "Boot",
                "Apache",
                "Flink",
                "ProcessFunction",
                "AI");

        // ProcessFunction gives us full control over each input event.
        // Here we do two things at the same time:
        // 1. emit (word, 1) to the main stream for downstream counting
        // 2. emit large words to a side output for auditing/debugging
        SingleOutputStreamOperator<Tuple2<String, Integer>> processed = words
                .process(new WordAuditProcessFunction());

        // Main stream output: records that continue through the normal pipeline.
        processed.print("[ProcessFunction-Main]");

        // Side output: special records that we want to inspect separately.
        processed.getSideOutput(LONG_WORDS_TAG).print("[ProcessFunction-LongWords]");

        // Flink uses lazy execution. Defining the pipeline is not enough;
        // we must explicitly submit it so the main output and side output
        // are both executed and printed to the console.
        env.execute("process-function-example");
    }

    private static final class WordAuditProcessFunction extends ProcessFunction<String, Tuple2<String, Integer>> {

        @Override
        public void processElement(String value, Context ctx, Collector<Tuple2<String, Integer>> out) {
            // Normalize the incoming value once so all downstream logic works
            // with a clean and predictable representation.
            String word = value.toLowerCase().trim();

            // Defensive guard: ignore blank or malformed values early.
            if (word.isBlank()) {
                return;
            }

            // Emit the normalized word to the main output stream. This keeps
            // the core counting pipeline simple and composable.
            out.collect(Tuple2.of(word, 1));

            // Side outputs are one of the main reasons engineers choose
            // ProcessFunction over map/flatMap. We can branch special records
            // without polluting the main data model.
            if (word.length() > 5) {
                ctx.output(LONG_WORDS_TAG, "Long word detected: " + word);
            }
        }
    }
}
