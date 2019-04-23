## HadoopStreaming
Set of Input Formats for Hadoop Streaming.

These classes specifically designed to allow you to pass __a lot of small__ files into hadoop streaming.</br>
One single map task will be able to process input size up to the amount in kb defined by the formula:
```
max(
  mapreduce.input.fileinputformat.split.minsize,
  min(mapreduce.input.fileinputformat.split.maxsize, dfs.blocksize)
)
```

---

Reading Avro:
```bash
$ hadoop jar -libjars streaming-1.0.jar \
    -inputformat com.haodemon.streaming.avro.CombinedAvroInputFormat \
    -input   <input>   \
    -output  <output>  \
    -mapper  <mapper>  \
    -reducer <reducer>
```

Reading Sequence:
```bash
$ hadoop jar -libjars streaming-1.0.jar \
    -inputformat com.haodemon.streaming.sequence.CombinedSequenceInputFormat \
    -input   <input>   \
    -output  <output>  \
    -mapper  <mapper>  \
    -reducer <reducer>
```


To build the package:
```bash
$ mvn package
```
