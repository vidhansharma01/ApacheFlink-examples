package com.example.demo.flink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.Locale;

public class WordCountJob {

    private static final String DEFAULT_TEXT = "Apache Flink word count example from Spring Boot project";

    public static void main(String[] args) throws Exception {
        String text = buildTextArgument(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(1);

        DataStream<String> input = env.fromData(text);

        input.flatMap((String line, org.apache.flink.util.Collector<Tuple2<String, Integer>> out) -> {
                    Arrays.stream(line.toLowerCase(Locale.ROOT).split("\\W+"))
                            .filter(token -> !token.isBlank())
                            .forEach(token -> out.collect(Tuple2.of(token, 1)));
                })
                .returns(org.apache.flink.api.common.typeinfo.Types.TUPLE(
                        org.apache.flink.api.common.typeinfo.Types.STRING,
                        org.apache.flink.api.common.typeinfo.Types.INT))
                .keyBy(value -> value.f0)
                .sum(1)
                .print();

        env.execute("remote-word-count");
    }

    private static String buildTextArgument(String[] args) {
        StringBuilder text = new StringBuilder();
        for (String arg : args) {
            if (text.length() > 0) {
                text.append(' ');
            }
            text.append(arg);
        }
        return text.length() == 0 ? DEFAULT_TEXT : text.toString();
    }
}
