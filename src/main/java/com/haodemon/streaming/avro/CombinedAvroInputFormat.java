package com.haodemon.streaming.avro;

import org.apache.avro.mapred.AvroAsTextInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.*;

import java.io.IOException;

public class CombinedAvroInputFormat extends CombineFileInputFormat<Text, Text> {
    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public RecordReader<Text, Text> getRecordReader(InputSplit split, JobConf conf, Reporter reporter) throws IOException {
        return new CombineFileRecordReader(conf, (CombineFileSplit)split, reporter, CombineAvroInputFormat.class);
    }

    private static class CombineAvroInputFormat extends CombineFileRecordReaderWrapper<Text, Text> {
        public CombineAvroInputFormat(CombineFileSplit split, Configuration conf, Reporter reporter, Integer idx) throws IOException {
            super(new AvroAsTextInputFormat(), split, conf, reporter, idx);
        }
    }
}
