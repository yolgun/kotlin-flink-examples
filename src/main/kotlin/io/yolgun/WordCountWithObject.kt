package io.yolgun

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.util.Collector

/**
 * Taken from Flink examples. Adapted to Kotlin. Original file can be found at
 * src/main/java/io/yolgun/WordCountOriginal
 *
 * At here I have used an object that implements FlatMapFunction.
 */
fun main(args: Array<String>) {
    val params = ParameterTool.fromArgs(args)
    val env = streamExecutionEnvironment()
    env.config.globalJobParameters = params

    val input = params["input"] ?:
            """C:\Users\yoldeta\Desktop\sandbox\kotlinflinkexamples\src\main\resources\sample.text"""
    val inputFullPath = "file:///$input"
    val text = env.readTextFile(inputFullPath)

    text.flatMap(Tokenizer)
            .keyBy(0)
            .sum(1)
            .print()

    env.execute("Streaming WordCountOriginal")
}

//data class WordCount(var word: String, var count: Int)

private fun streamExecutionEnvironment(): StreamExecutionEnvironment {
    return StreamExecutionEnvironment.getExecutionEnvironment()
}

private object Tokenizer : FlatMapFunction<String, Tuple2<String, Int>> {
    override fun flatMap(value: String, out: Collector<Tuple2<String, Int>>) =
            value.split("\\W+".toRegex())
                    .asSequence()
                    .filter { it.isNotEmpty() }
                    .map { it.toLowerCase() }
                    .forEach { out.collect(Tuple2(it, 1)) }
}
