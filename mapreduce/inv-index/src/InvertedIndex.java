import java.io.FileInputStream;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.HashSet;

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
    private final static Pattern validWordPattern = Pattern.compile("[a-zA-Z0-9]+");

    public static String urlFromPath(String path) {
        String[] parts = path.split("/");

        boolean foundDomain = false;
        StringBuilder urlBuilder = new StringBuilder();
        for (String part : parts) {
            foundDomain = foundDomain | part.contains(".");
            if (!foundDomain)
                continue;

            urlBuilder.append(part);
            urlBuilder.append("/");
        }

        return urlBuilder.substring(0, urlBuilder.length() - 1);
    }

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
        private Text url = new Text();
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Document doc = Jsoup.parse(value.toString());
            String text = doc.body().text();

            FileSplit split = (FileSplit) context.getInputSplit();
            url.set(urlFromPath(split.getPath().toString()));

            StringTokenizer itr = new StringTokenizer(text);
            while (itr.hasMoreTokens()) {
                String temp = itr.nextToken();
                temp = temp.replaceAll("(\"|\'|\\[|\\]|\\(|\\)|\\$|#|\\?|!|\\*|\\.|,|-|¿|¡|%|\\+)", "");

                Matcher matcher = validWordPattern.matcher(temp);
                if (matcher.matches()) {
                    word.set(temp);
                    context.write(word, url);
                }
            }
        }
    }

    public static class PathsReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashSet<String> seenUrls = new HashSet<String>();
            StringBuilder stringBuilder = new StringBuilder();

            for (Text path : values) {
                String temp = path.toString();
                if (!seenUrls.contains(temp)) {
                    seenUrls.add(temp);
                    stringBuilder.append(temp);
                    stringBuilder.append('|');
                }
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
        FileInputFormat.setInputDirRecursive(job, true);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}