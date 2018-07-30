package word_scores;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.giraph.writable.tuple.IntIntWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WordScores {
    private static final String WORD_MATCH_REGEX = "\\b[a-zA-Z]+['a-zA-Z]*\\b";
    private static String learningDataPath = "learning.txt";

    public static void main(String[] args) throws Exception {
        String mode = args[2];

        if (mode.equals("1")) {
            runWordScores(args);
        } else if (mode.equals("2")) {
            runReviewScores(args);
        }
    }

    private static void runWordScores(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word scores");
        job.setJarByClass(WordScores.class);
        job.setMapperClass(TokenizerMapper.class);

        // Combiner is responsible for local (every map output item) aggregation
        job.setCombinerClass(IntIntSumReducer.class);

        // Reducer comes after internal shuffle/sort
        job.setReducerClass(IntIntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntIntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static void runReviewScores(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        learningDataPath = args[3];

        Job job = Job.getInstance(conf, "word scores");
        job.setJarByClass(WordScores.class);

        job.setMapperClass(ReviewMapper.class);

        // Combiner is responsible for local (every map output item) aggregation
        job.setCombinerClass(ReviewScoreReducer.class);

        // Reducer comes after internal shuffle/sort
        job.setReducerClass(ReviewScoreReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntIntWritable.class);

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Split all the data into blocks of 4 lines (i.e. a review)
        job.setInputFormatClass(MultiLineInputFormat.class);
        NLineInputFormat.setNumLinesPerSplit(job, 4);
        FileInputFormat.addInputPath(job, new Path(args[0]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    // Mapper responsible for mapping split to review pos_word_score, neg_word_score
    // Splits review into words, Does lookup on learning list to get the word rating
    public static class ReviewMapper extends Mapper<Object, Text, Text, IntIntWritable> {
        public static final Log log = LogFactory.getLog(ReviewMapper.class);
        private static String learningData;
        private Text idKey = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Path pt = new Path("hdfs://node-master:9000/user/hadoop/" + learningDataPath);

            FileSystem fs = FileSystem.get(context.getConfiguration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));

            learningData = org.apache.commons.io.IOUtils.toString(br);

            super.setup(context);
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            List<String> reviewLines = Arrays.asList(value.toString().split("\\n"));

            String id = Arrays.asList(reviewLines.get(1).split(" ")).get(1);
            idKey.set(id);

            Pattern pattern = Pattern.compile(WORD_MATCH_REGEX);
            Matcher matcher;

            matcher = pattern.matcher(reviewLines.get(3).replace("review/text: ", ""));

            List<String> reviewWords = new ArrayList<>();

            while (matcher.find()) {
                reviewWords.add(matcher.group());
            }

            reviewWords.forEach(reviewWord -> {
                String wordLine = Arrays.stream(learningData.split("\\n"))
                        .filter(line -> line.contains(reviewWord.toLowerCase())).findFirst().orElse("");

                Pattern patternWordLine = Pattern.compile("[0-9]+,[0-9]+");
                Matcher wordLineMatcher = patternWordLine.matcher(wordLine);
                List<String> scores = new ArrayList<>();

                while (wordLineMatcher.find()) {
                    scores.add(wordLineMatcher.group());
                }

                // If the word does not exist in the learning list, it makes no impact on review
                if (scores.size() == 0) {
                    try {
                        context.write(idKey, new IntIntWritable(0, 0));
                    } catch (IOException | InterruptedException e) {
                        e.printStackTrace();
                    }
                } else {
                    scores = Arrays.asList(scores.get(0).split(","));

                    try {
                        context.write(idKey, new IntIntWritable(Integer.parseInt(scores.get(0)), Integer.parseInt(scores.get(1))));
                    } catch (IOException | InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
        }
    }

    // Mapper responsible for splitting input into words
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntIntWritable> {
        private Text word = new Text();

        private String filename;
        private String POS_FILENAME = "pos.txt";
        private String NEG_FILENAME = "neg.txt";

        // Currently, there's no other solution apart from reflection to get currently inputted file (source)
        // of a split
        @Override
        protected void setup(Context context) throws IOException {

            InputSplit split = context.getInputSplit();
            Class<? extends InputSplit> splitClass = split.getClass();

            FileSplit fileSplit = null;
            if (splitClass.equals(FileSplit.class)) {
                fileSplit = (FileSplit) split;
            } else if (splitClass.getName().equals(
                    "org.apache.hadoop.mapreduce.lib.input.TaggedInputSplit")) {

                try {
                    Method getInputSplitMethod = splitClass
                            .getDeclaredMethod("getInputSplit");
                    getInputSplitMethod.setAccessible(true);
                    fileSplit = (FileSplit) getInputSplitMethod.invoke(split);
                } catch (Exception e) {
                    // wrap and re-throw error
                    throw new IOException(e);
                }
            }

            filename = fileSplit.getPath().getName();
        }

        // Downcase the words
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            List<String> words = new ArrayList<>();

            Pattern pattern = Pattern.compile(WORD_MATCH_REGEX);
            Matcher matcher = pattern.matcher(value.toString().toLowerCase());

            while (matcher.find()) {
                words.add(matcher.group());
            }

            for (String currWord : words) {
                IntIntWritable posNeg;

                word.set(currWord);

                if (filename.equals(POS_FILENAME)) {
                    posNeg = new IntIntWritable(1, 0);
                    context.write(word, posNeg);
                } else if (filename.equals(NEG_FILENAME)) {
                    posNeg = new IntIntWritable(0, 1);
                    context.write(word, posNeg);
                }
            }
        }
    }

    // Reducer already received for each key aggregated values
    // All the programmer needs to spit out the output of such each aggregation (in our case, sum up values)
    public static class IntIntSumReducer extends Reducer<Text, IntIntWritable, Text, IntIntWritable> {
        public void reduce(Text key, Iterable<IntIntWritable> values, Context context) throws IOException, InterruptedException {
            int[] sum = new int[2];

            for (IntIntWritable val : values) {
                sum[0] += val.getLeft().get();
                sum[1] += val.getRight().get();
            }

            IntIntWritable result = new IntIntWritable(sum[0], sum[1]);
            context.write(key, result);
        }
    }

    public static class ReviewScoreReducer extends Reducer<Text, IntIntWritable, Text, IntIntWritable> {
        public void reduce(Text key, Iterable<IntIntWritable> values, Context context) throws IOException, InterruptedException {
            int[] sum = new int[2];
            int counter = 0;

            for (IntIntWritable val : values) {
                sum[0] += val.getLeft().get();
                sum[1] += val.getRight().get();
                counter++;
            }

            sum[0] = sum[0] / counter;
            sum[1] = sum[1] / counter;

            IntIntWritable result = new IntIntWritable(sum[0], sum[1]);
            context.write(key, result);
        }
    }
}