package hadoop.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class MatrixTranspost {
    //矩阵中某个元素的表示方法：行号，列号，值
    private static int matrix1Row = 3;   //矩阵1的行数
    private static int matrix1Col = 3;   //矩阵1的列数
    private static int matrix2Row = 3;   //矩阵2的行数
    private static int matrix2Col = 2;   //矩阵2的列数

    public static class SplitAndTranspostMapper extends Mapper<LongWritable, Text, Text, Text>{
        private String fileName;    //分片来源的文件名，用于判断分片来自哪个文件

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            FileSplit split = (FileSplit) context.getInputSplit();
            fileName = split.getPath().getName();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            if (matrix1Col!=matrix2Row)
                return;
            if (value==null||"".equals(value))
                return;
            String[] elements = value.toString().split(",");

            if (fileName.equals("matrix1")){
//                if (elements.length<matrix1Col)
//                    return;
                for (int j=1;j<=matrix1Col;j++){
                    System.out.println("mapper:"+"matrix1,"+"key="+elements[0]+","+j+"  value="+elements[1]+","+elements[2]);
                    context.write(new Text(elements[0]+","+j),new Text("matrix1,"+elements[1]+","+elements[2]));    // val=matrix1,column,value
                }
            }else if (fileName.equals("matrix2")){
//                if (elements.length<matrix2Col)
//                    return;
                for (int i=1;i<=matrix2Row;i++){
                    System.out.println("mapper:"+"matrix2,"+"key="+i+","+elements[1]+"  value="+elements[0]+","+elements[2]);
                    context.write(new Text(i+","+elements[1]),new Text("matrix2,"+elements[0]+","+elements[2]));
                }
            }
        }
    }

    public static class InnerProductReduce extends Reducer<Text,Text,Text,IntWritable>{

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            if (matrix1Col!=matrix2Row)
                return;
            int[] rowArray = new int[matrix1Col];
            int[] colArray = new int[matrix2Row];
            if (values==null||"".equals(values))
                return;
            //判断key代表的位置是否在结果矩阵中
            if (!isInResultMatrix(key))
                return;

            for (Text val:values){
                System.out.println("reducer:key="+key.toString()+" value="+val);
                String[] valStrs = val.toString().split(",");
//                if (valStrs.length<3)
//                    return;
                if ("matrix1".equals(valStrs[0])){
                    int rowArrayIndex = Integer.parseInt(valStrs[1].toString())-1;    //获取该值的列数
                    rowArray[rowArrayIndex] = Integer.parseInt(valStrs[2]);     //将数值根据列数放到相应的数组中
                }else if ("matrix2".equals(valStrs[0])){
                    int colArrayIndex = Integer.parseInt(valStrs[1].toString())-1;
                    colArray[colArrayIndex] = Integer.parseInt(valStrs[2].toString());
                }
            }
            int sum = 0;
            for (int i=0;i<matrix1Col;i++){
                sum = sum + rowArray[i]*colArray[i];
            }
            context.write(key,new IntWritable(sum));
        }

        public boolean isInResultMatrix(Text position){
            if (position==null||"".equals(position))
                return false;
            String[] pStr = position.toString().split(",");
            if (pStr.length<2)
                return false;
            int rowIndex = Integer.parseInt(pStr[0]);
            int colIndex = Integer.parseInt(pStr[1]);
            if (rowIndex<=matrix1Row && colIndex<=matrix2Col)
                return true;
            else
                return false;
        }
    }



    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "matrix multi");
        job.setJarByClass(MatrixTranspost.class);
        job.setMapperClass(SplitAndTranspostMapper.class);

        job.setReducerClass(InnerProductReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
