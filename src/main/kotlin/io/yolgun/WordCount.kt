package io.yolgun

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import java.io.File

/**
 * Taken from Flink examples. Adapted to Kotlin. Original file can be found at
 * src/main/java/io/yolgun/WordCount
 *
 * Implements the "WordCount" program that computes a simple word occurrence
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
    val env = streamExecutionEnvironmentRemote()
    env.config.globalJobParameters = params

    val input = params["input"] ?:
            """C:\Users\yoldeta\Desktop\sandbox\kotlinflinkexamples\src\main\resources\sample.text"""
    val inputFullPath = "file:///$input"
    val text = env.readTextFile(inputFullPath)

    text.flatMap(Tokenizer)
            .keyBy(0)
            .sum(1)
            .print()

    env.execute("Streaming WordCount")
}

private fun streamExecutionEnvironment(): StreamExecutionEnvironment {
    return StreamExecutionEnvironment.getExecutionEnvironment()
}

private fun streamExecutionEnvironmentRemote(): StreamExecutionEnvironment {
    return StreamExecutionEnvironment.createRemoteEnvironment(
            "127.0.0.1",
            6123,
            """C:\Users\yoldeta\Desktop\sandbox\kotlinflinkexamples\target\kotlin-flink-examples-1.0-SNAPSHOT-jar-with-dependencies.jar"""
    )
}

object Tokenizer : FlatMapFunction<String, Tuple2<String, Int>> {
    override fun flatMap(value: String, out: Collector<Tuple2<String, Int>>) {
        value.split("\\W+".toRegex())
                .asSequence()
                .filter { it.isNotEmpty() }
                .map {
                    Thread.sleep(1000)
                    it.toLowerCase()
                }
                .forEach { out.collect(Tuple2(it, 1)) }
    }
}
