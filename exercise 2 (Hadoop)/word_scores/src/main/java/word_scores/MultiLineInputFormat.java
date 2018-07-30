package word_scores;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;

// Input splitter for Apache Hadoop. Splits input into defined
// line blocks (i.e. into 4 line blocks)
public class MultiLineInputFormat extends NLineInputFormat {
    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit genericSplit, TaskAttemptContext context) {
        context.setStatus(genericSplit.toString());
        return new MultiLineRecordReader();
    }

    public static class MultiLineRecordReader extends RecordReader<LongWritable, Text> {
        private int NUMBER_LINES_TO_PROCESS;
        private LineReader in;
        private LongWritable key;
        private Text value = new Text();
        private long start = 0;
        private long end = 0;
        private long pos = 0;
        private int maximumLineLength;

        @Override
        public void close() throws IOException {
            if (in != null) {
                in.close();
            }
        }

        @Override
        public LongWritable getCurrentKey() {
            return key;
        }

        @Override
        public Text getCurrentValue() {
            return value;
        }

        @Override
        public float getProgress() {
            if (start == end) {
                return 0.0f;
            } else {
                return Math.min(1.0f, (pos - start) / (float) (end - start));
            }
        }

        @Override
        public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
            NUMBER_LINES_TO_PROCESS = getNumLinesPerSplit(context);
            FileSplit split = (FileSplit) genericSplit;
            final Path file = split.getPath();
            Configuration conf = context.getConfiguration();
            this.maximumLineLength = conf.getInt("mapreduce.input.linerecordreader.line.maxlength", Integer.MAX_VALUE);
            FileSystem fs = file.getFileSystem(conf);
            start = split.getStart();
            end = start + split.getLength();
            boolean skipFirstLine = false;
            FSDataInputStream filein = fs.open(split.getPath());

            if (start != 0) {
                skipFirstLine = true;
                --start;
                filein.seek(start);
            }

            in = new LineReader(filein, conf);

            if (skipFirstLine) {
                start += in.readLine(new Text(), 0, (int) Math.min((long) Integer.MAX_VALUE, end - start));
            }

            this.pos = start;
        }

        @Override
        public boolean nextKeyValue() throws IOException {

            if (key == null) {
                key = new LongWritable();
            }

            key.set(pos);

            if (value == null) {
                value = new Text();
            }

            value.clear();

            final Text endline = new Text("\n");

            int newSize = 0;

            for (int i = 0; i < NUMBER_LINES_TO_PROCESS; i++) {
                Text v = new Text();
                while (pos < end) {
                    newSize = in.readLine(v, maximumLineLength, Math.max((int) Math.min(Integer.MAX_VALUE, end - pos), maximumLineLength));
                    value.append(v.getBytes(), 0, v.getLength());
                    value.append(endline.getBytes(), 0, endline.getLength());
                    if (newSize == 0) {
                        break;
                    }
                    pos += newSize;
                    if (newSize < maximumLineLength) {
                        break;
                    }
                }
            }

            if (newSize == 0) {
                key = null;
                value = null;
                return false;
            } else {
                return true;
            }
        }
    }

}
