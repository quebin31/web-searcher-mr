import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class WholeFileRecordReader extends RecordReader<Object, Text> {
    private FileSplit split;
    private Configuration conf;

    private final Object dummy = new Object();
    private final Text currValue = new Text();
    private boolean fileProcessed = false;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        this.split = (FileSplit) split;
        this.conf = context.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (this.fileProcessed) {
            return false;
        }

        Path path = this.split.getPath(); 
        FileSystem fs = path.getFileSystem(conf);
        FSDataInputStream in = null;
        try {
            in = fs.open(path);
            byte[] bytes = IOUtils.readFullyToByteArray(in);
            this.currValue.set(bytes);
        } catch (Exception e) {
            System.out.println("Error reading whole file: " + e.toString());
        } finally {
            if (in != null)
                IOUtils.closeStream(in);
        }

        this.fileProcessed = true;
        return true;
    }

    @Override
    public Object getCurrentKey() throws IOException, InterruptedException {
        return this.dummy;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return this.currValue;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {
    }

}
