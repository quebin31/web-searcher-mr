import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class InitPageRank {
    public static boolean isAbsoluteUrl(String url) {
        return url.startsWith("http");
    }

    public static String cleanUrl(String url) {
        return url.replaceAll("(http://|https://|\\?.*)", "");
    }

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

    public static class OutLinkMapper extends Mapper<Object, Text, Text, Text> {
        private Text selfUrlAndRank = new Text();
        private Text outLink = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Document doc = Jsoup.parse(value.toString());
            Elements links = doc.body().getElementsByTag("a");

            FileSplit split = (FileSplit) context.getInputSplit();
            String pathString = split.getPath().toString();
            String selfUrl = urlFromPath(pathString);

            selfUrlAndRank.set(selfUrl.concat("|1"));

            HashSet<String> seenUrls = new HashSet<String>();
            for (Element link : links) {
                String url = link.attr("href");
                if (!isAbsoluteUrl(url))
                    continue;

                url = cleanUrl(url);
                if (!seenUrls.contains(url)) {
                    seenUrls.add(url);
                    outLink.set(url);
                    context.write(selfUrlAndRank, outLink);
                }
            }
        }
    }

    public static class OutLinksReducer extends Reducer<Text, Text, Text, Text> {
        private Text outLinks = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder stringBuilder = new StringBuilder();

            for (Text outLink : values) {
                stringBuilder.append(outLink.toString());
                stringBuilder.append('|');
            }

            outLinks.set(stringBuilder.substring(0, stringBuilder.length() - 1));
            context.write(key, outLinks);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "init-page-rank");

        job.setJarByClass(InitPageRank.class);
        job.setMapperClass(OutLinkMapper.class);
        job.setReducerClass(OutLinksReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(WholeFileInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.setInputDirRecursive(job, true);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
