package com.haodemon.streaming.sequence;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.CombineFileInputFormat;
import org.apache.hadoop.mapred.lib.CombineFileRecordReader;
import org.apache.hadoop.mapred.lib.CombineFileRecordReaderWrapper;
import org.apache.hadoop.mapred.lib.CombineFileSplit;

import java.io.IOException;

public class CombinedSequenceInputFormat extends CombineFileInputFormat<Text, Text> {
    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public RecordReader<Text, Text> getRecordReader(InputSplit inputSplit, JobConf jobConf, Reporter reporter) throws IOException {
        return new CombineFileRecordReader(jobConf, (CombineFileSplit)inputSplit, reporter, CombineSequenceInputFormat.class);
    }

    private class CombineSequenceInputFormat extends CombineFileRecordReaderWrapper<Text, Text> {
        public CombineSequenceInputFormat(CombineFileSplit split, Configuration conf, Reporter reporter, Integer idx) throws IOException {
            super(new SequenceFileAsTextInputFormat(), split, conf, reporter, idx);
        }
    }
}
