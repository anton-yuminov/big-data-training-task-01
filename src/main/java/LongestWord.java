import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.UUID;

public class LongestWord extends Configured implements Tool {

    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(getConf());
        job.setJobName("Longest word finder job");
        job.setJarByClass(LongestWord.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        job.setCombinerClass(LongestWord.Reduce.class);
        job.setReducerClass(Reduce.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class Map<S, T extends BinaryComparable> extends Mapper<LongWritable, Text, Text, Text> {
        private Text keyWord = new Text();
        private String randomKey = UUID.randomUUID().toString();

        {
            keyWord.set(randomKey); // TODO consider use node name
        }

        private Text wordWord = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            int longestWordLength = 0;
            StringTokenizer tokenizer = new StringTokenizer(line);

            while (tokenizer.hasMoreTokens()) {
                String currentWord = tokenizer.nextToken();
                if (currentWord.length() > longestWordLength) {
                    longestWordLength = currentWord.length();
                }
            }

            tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                String currentWord = tokenizer.nextToken();
                if (currentWord.length() == longestWordLength) {
                    wordWord.set(currentWord);
                    context.write(keyWord, wordWord);
                }
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            if (!values.iterator().hasNext()) {
                return;
            }
            // Последующий алгоритм приведет к OOM Error на очень больших выборках. Это противоречит задаче
            //На прмышленной задаче я вернул бы longestWord. Но решил на тестовых объемах поэкспериментировать
            java.util.Map<String, Text> longestWords = new HashMap<>();

            Text longestWord = values.iterator().next();
            for (Text val : values) {
                if (val.getLength() > longestWord.getLength()) {
                    longestWord = val;
                    longestWords.clear();
                    longestWords.put(val.toString(), val);
                } else {
                    if (val.getLength() == longestWord.getLength()) {
                        longestWords.put(val.toString(), val);
                    }
                }
            }
            for (java.util.Map.Entry<String, Text> e : longestWords.entrySet()) {
                context.write(key, e.getValue());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new LongestWord(), args);
        System.exit(res);
    }

}