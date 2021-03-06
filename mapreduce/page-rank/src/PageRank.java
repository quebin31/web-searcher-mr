import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageRank {
    private static final long MAX_SPLIT_SIZE = 128 * 1024 * 1024;

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.out.println("Error: invalid number of arguments");
            System.out.println("Usage: ... <input-dir> <temp-dir> <output-dir> <no-iterations>");
            return;
        }

        Configuration conf = new Configuration();
        conf.setLong(FileInputFormat.SPLIT_MAXSIZE, MAX_SPLIT_SIZE);

        Path inputPath = new Path(args[0]);
        Path tempPath = new Path(args[1]);
        Path outputPath = new Path(args[2]);

        int noIterations = Integer.parseUnsignedInt(args[3]);

        // Initial job to get outlinks (init page rank)
        Job initJob = Job.getInstance(conf, "init-page-rank");
        initJob.setJarByClass(InitPageRank.class);
        initJob.setMapperClass(InitPageRank.OutLinkMapper.class);
        initJob.setReducerClass(InitPageRank.OutLinksReducer.class);
        initJob.setOutputKeyClass(Text.class);
        initJob.setOutputValueClass(Text.class);
        initJob.setInputFormatClass(WholeFileInputFormat.class);
        FileInputFormat.addInputPath(initJob, inputPath);
        FileInputFormat.setInputDirRecursive(initJob, true);
        FileOutputFormat.setOutputPath(initJob, tempPath);
        initJob.waitForCompletion(true);

        // Subsequent job to calculate page rank values iteratively
        FileSystem fs = tempPath.getFileSystem(conf);
        for (int it = 0; it < noIterations; ++it) {
            Job calcJob = Job.getInstance(conf, "calc-page-rank");
            calcJob.setJarByClass(CalcPageRank.class);
            calcJob.setMapperClass(CalcPageRank.MapOp.class);
            calcJob.setReducerClass(CalcPageRank.ReduceOp.class);
            calcJob.setOutputKeyClass(Text.class);
            calcJob.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(calcJob, tempPath);
            FileOutputFormat.setOutputPath(calcJob, outputPath);
            calcJob.waitForCompletion(true);

            if (it != noIterations - 1) {
                fs.delete(tempPath, true);
                fs.rename(outputPath, tempPath);
            }
        }
    }
}
