import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
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


public class WordCounter extends Configured implements Tool {
    public static class Map extends Mapper<LongWritable, Text, Text, LongWritable> {
        private final static LongWritable one = new LongWritable(1);
        public void map(LongWritable key, Text text, Context context) throws IOException, InterruptedException {
			String[] words = text.toString().split("[\\p{P}   \\| \\t\\n\\r\\s]+");
            for (int i = 1; i < words.length; ++i) {
                if (words[i].equals(word1)   &&  words[i+1].equals(word2) && words[i+2].equals(word3)) {
                    words[i] = words[i].toLowerCase();
                    context.write(new Text(words[i]+words[i+1]+words[i+2]), one);
                }
				if (words[i].equals(word1)   &&  words[i+1].equals(word2) && !words[i+2].equals(word3)) {
                    words[i] = words[i].toLowerCase();
                    context.write(new Text(words[i]+words[i+1]), one);
                }
            }
        }
    }


    public static class Reduce extends Reducer<Text, LongWritable, Text, LongWritable> {
        public void reduce(Text key, Iterable<LongWritable> value, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (LongWritable val: value) {
                sum += val.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }


    public int run(String[] args) throws Exception {
        if(args.length != 5) {
            System.err.println("INPUT ERROR! ");
            return -1;
        }
		String word1 = new String(args[2]);
		String word2 = new String(args[3]);
		String word3 = new String(args[4]);
        Job job = new Job(getConf());
        job.setJarByClass(WordCounter.class);
        job.setJobName("wiki");
        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new WordCounter(), args);
        System.exit(result);
    }
}
