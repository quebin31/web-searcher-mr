import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CalcPageRank {
    public static final double DAMPING_FACTOR = 0.85;

    public static class MapOp extends Mapper<Object, Text, Text, Text> {
        private Text outLinkText = new Text();
        private Text outRankValue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] urlRankAndOutLinks = value.toString().split("\t");

            String[] urlRank = urlRankAndOutLinks[0].split("\\|");
            String url = urlRank[0];
            double rank = Double.parseDouble(urlRank[1]);

            String[] outLinks;
            if (urlRankAndOutLinks.length == 2) {
                outLinks = urlRankAndOutLinks[1].split("\\|");
            } else {
                outLinks = new String[0];
            }

            for (String outLink : outLinks) {
                outLinkText.set(outLink);
                double outRank = rank / (double) outLinks.length;
                outRankValue.set(String.valueOf(outRank));

                context.write(outLinkText, outRankValue);
            }

            if (urlRankAndOutLinks.length == 2) {
                context.write(new Text(url), new Text(urlRankAndOutLinks[1]));
            } else {
                context.write(new Text(url), new Text(""));
            }
        }
    }

    public static class ReduceOp extends Reducer<Text, Text, Text, Text> {
        private Text outLinks = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double pageRank = 0;

            for (Text value : values) {
                String temp = value.toString();

                if (temp.contains("|") || temp.isEmpty()) {
                    outLinks.set(temp);
                } else {
                    double rank = Double.parseDouble(temp);
                    pageRank += rank;
                }
            }

            pageRank = 1 - DAMPING_FACTOR + (DAMPING_FACTOR * pageRank);

            String url = key.toString();
            String urlRank = url.concat("|").concat(String.valueOf(pageRank));
            context.write(new Text(urlRank), outLinks);
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "calc-page-rank");
        job.setJarByClass(CalcPageRank.class);
        job.setMapperClass(MapOp.class);
        job.setReducerClass(ReduceOp.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}