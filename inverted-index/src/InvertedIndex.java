import java.io.IOException;
import java.util.StringTokenizer;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndex {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

        private Text word = new Text();
        private Text docPath = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Document doc = Jsoup.parse(value.toString());
            String text = doc.body().text();

            FileSplit split = (FileSplit) context.getInputSplit();
            docPath.set(split.getPath().toString());

            StringTokenizer itr = new StringTokenizer(text);
            while (itr.hasMoreTokens()) {
                String temp = itr.nextToken();
                temp = temp.replaceAll("(\"|\'|\\[|\\]|\\(|\\)|\\$|#|\\?|!|\\*|\\.|,|-|¿|¡|%)", "");

                if (temp.isEmpty()) {
                    continue;
                }

                word.set(temp);
                context.write(word, docPath);
            }
        }
    }

    public static class PathsReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder stringBuilder = new StringBuilder();

            for (Text path : values) {
                stringBuilder.append(path.toString());
                stringBuilder.append(';');
            }

            result.set(stringBuilder.substring(0, stringBuilder.length() - 1));
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "inverted-index");
        job.setJarByClass(InvertedIndex.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(PathsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(WholeFileInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}