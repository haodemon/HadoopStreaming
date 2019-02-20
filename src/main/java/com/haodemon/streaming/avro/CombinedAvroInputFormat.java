package com.haodemon.streaming.avro;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.mapred.AvroAsTextInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.*;

import java.io.IOException;

public class CombinedAvroInputFormat extends CombineFileInputFormat<Text, Text> {
    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public RecordReader<Text, Text> getRecordReader(InputSplit split,
                                                    JobConf conf,
                                                    Reporter reporter) throws IOException {
        return new CombineFileRecordReader(conf, (CombineFileSplit)split, reporter, CombineAvroInputFormat.class);
    }

    private static class CombineAvroInputFormat extends CombineFileRecordReaderWrapper<Text, Text> {
        public CombineAvroInputFormat(CombineFileSplit split,
                                      Configuration conf,
                                      Reporter reporter,
                                      Integer idx) throws IOException {
            super(new CustomAvroAsTextInputFormat(), split, conf, reporter, idx);
        }
    }

    /**
     * Wrapped to avoid exceptions for avro with invalid sync data
     */
    private static class CustomAvroAsTextInputFormat extends AvroAsTextInputFormat {
        private static final String EXCEPTION_INVALID_SYNC = "java.io.IOException: Invalid sync!";
        @Override
        public RecordReader<Text, Text> getRecordReader(InputSplit split,
                                                        JobConf job,
                                                        Reporter reporter) throws IOException {
            RecordReader<Text, Text> rr = super.getRecordReader(split, job, reporter);
            return new RecordReader<Text, Text>() {
                @Override
                public boolean next(Text key, Text ignore) throws IOException {
                    try {
                        return rr.next(key, ignore);
                    }
                    catch (AvroRuntimeException e) {
                        if (e.getMessage().matches(EXCEPTION_INVALID_SYNC)) return false;
                        throw e;
                    }
                }

                @Override
                public Text createKey() {
                    return rr.createKey();
                }

                @Override
                public Text createValue() {
                    return rr.createValue();
                }

                @Override
                public long getPos() throws IOException {
                    return rr.getPos();
                }

                @Override
                public void close() throws IOException {
                    rr.close();
                }

                @Override
                public float getProgress() throws IOException {
                    return rr.getProgress();
                }
            };
        }
    }
}
