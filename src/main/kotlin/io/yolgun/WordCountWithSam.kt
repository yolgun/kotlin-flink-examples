package io.yolgun

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

/**
 * Taken from Flink examples. Adapted to Kotlin. Original file can be found at
 * src/main/java/io/yolgun/WordCountOriginal
 *
 * Implements the "WordCountOriginal" program that computes a simple word occurrence
 * histogram over text files in a streaming fashion.
 *
 * The input is a plain text file with lines separated by newline characters.
 *
 * This example shows how to:
 *
 *  * write a simple Flink Streaming program,
 *  * use tuple data types,
 *  * write and use user-defined functions.
 *
 */
fun main(args: Array<String>) {
    val params = ParameterTool.fromArgs(args)
    val env = streamExecutionEnvironment()
    env.config.globalJobParameters = params

    val input = params["input"] ?:
            """C:\Users\yoldeta\Desktop\sandbox\kotlinflinkexamples\src\main\resources\sample.text"""
    val inputFullPath = "file:///$input"
    val text = env.readTextFile(inputFullPath)

    val samTokenizer = FlatMapFunction<String, Tuple2<String, Int>> { value, out ->
        value.split("\\W+".toRegex())
                .asSequence()
                .filter { it.isNotEmpty() }
                .map { it.toLowerCase() }
                .forEach { out.collect(Tuple2(it, 1)) }
    }

    text.flatMap(samTokenizer)
            .keyBy(0)
            .sum(1)
            .print()

    env.execute("Streaming WordCountOriginal")
}

private fun streamExecutionEnvironment(): StreamExecutionEnvironment {
    return StreamExecutionEnvironment.getExecutionEnvironment()
}
