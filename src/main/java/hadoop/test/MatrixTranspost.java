package hadoop.test;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MatrixTranspost {
    //矩阵一行：行号   值，值，值，值。。
    public static class SplitAndTranspostMapper extends Mapper<LongWritable, Text, Text, Text>{

        private int rowCount = 2;
        private int colCount = 2;
        private int rowIndex = 1;
        private int colIndex = 1;

        private String fileName;

        private Text outKey = new Text();
        private Text outVal = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            FileSplit split = (FileSplit) context.getInputSplit();
            fileName = split.getPath().getName();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] elements = value.toString().split(",");
            if (fileName.equals("matrix1")){
                for (int i=1;i<=rowCount;i++){
                    for (int j=1;j<=elements.length;j++){
                        outKey.set(j+","+i);
                        outVal.set();
                    }
                }
            }else {

            }
        }
    }

    public static class RebuildTranspostReduce extends Reducer<Text,IntWritable,Text,IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

        }
    }

}
