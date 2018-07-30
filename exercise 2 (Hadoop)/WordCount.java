package word_count;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WordCount {
    // matches any string with at least one alphabetical character that may be followed by an apostrophe and some more
    // letters. Matches only if the "word" is surrounded by word-boundary characters (\b).
    private static final String WORD_MATCH_REGEX = "\\b[a-zA-Z]+['a-zA-Z]*\\b";

    //
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);

        // Combiner is responsible for local (every map output item) aggregation
        job.setCombinerClass(IntSumReducer.class);

        // Reducer comes after internal shuffle/sort
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    // Mapper node(s) receive "chunks", (i.e. single lines of text with the default input format). We split the string
    // into single words, and send out a tuple of type <word,1> for every word that has been found.
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        //Initializations
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            // Define matcher and pattern. Apply regex onto input string, and put every found word into the list
            // of words.
            List<String> words = new ArrayList<>();
            Pattern pattern = Pattern.compile(WORD_MATCH_REGEX);
            Matcher matcher = pattern.matcher(value.toString());

            while (matcher.find()) {
                words.add(matcher.group());
            }

            // For every word we found, output a tuple of type <word, 1>.
            for (String string : words) {
                word.set(string);
                context.write(word, one);
            }
        }
    }

    // Reducer already received for each key aggregated values
    // All the programmer needs to spit out the output of such each aggregation (in our case, sum up values)
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;

            // reduce node receives list of all values for certain key. Sum up amount of tuples with this key.
            for (IntWritable val : values) {
                sum += val.get();
            }

            // For every key(i.e. word), put out tuple <word, amountOfTuplesWithKey==word>
            result.set(sum);
            context.write(key, result);
        }
    }
}