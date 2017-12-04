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
    //矩阵中某个元素的表示方法：行号，列号，值；也即：前两个是坐标，第三个是值
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
            if (elements.length<3)
                return;
            if (fileName.equals("matrix1")){
                //将矩阵中的一个元素复制多份（份数=矩阵1的列），因为这些值参与了多次（份数=矩阵1的列）与矩阵2对应元素的乘法；
                //map输出的键为该元素参与计算的“输出矩阵”的坐标，map输出的值为：矩阵名称，元素所在矩阵1中的列号，元素值
                for (int j=1;j<=matrix1Col;j++){
                    //map的key=[行号，矩阵1的列的全部值]
                    System.out.println("mapper:"+"matrix1,"+"key="+elements[0]+","+j+"  value="+elements[1]+","+elements[2]);
                    context.write(new Text(elements[0]+","+j),new Text("matrix1,"+elements[1]+","+elements[2]));    // val=matrix1,column,value
                }
            }else if (fileName.equals("matrix2")){
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
            int[] rowArray = new int[matrix1Col];   //用于保存结果矩阵key所代表的坐标（row,col）所用到的来自矩阵1中的值，元素在矩阵中的列数与数组的下标相对应（row-1=index）
            int[] colArray = new int[matrix2Row];   //用于保存结果矩阵key所代表的坐标（row,col）所用到的来自矩阵2中的值，元素在矩阵中的列数与数组的下标相对应（col-1=index）
            if (values==null||"".equals(values))
                return;
            //判断key代表的位置是否在“结果矩阵”中
            if (!isInResultMatrix(key))
                return;

            for (Text val:values){
                System.out.println("reducer:key="+key.toString()+" value="+val);
                //第一位是所属的矩阵名称，第二位是行号（来自矩阵1）或者列号（来自矩阵2），第三位是元素值
                String[] valStrs = val.toString().split(",");
                if (valStrs.length<3)
                    return;
                if ("matrix1".equals(valStrs[0])){
                    int rowArrayIndex = Integer.parseInt(valStrs[1].toString())-1;    //获取该值的列数（矩阵从1开始，数组从0开始）
                    rowArray[rowArrayIndex] = Integer.parseInt(valStrs[2]);     //将数值根据列数放到相应的数组中
                }else if ("matrix2".equals(valStrs[0])){
                    int colArrayIndex = Integer.parseInt(valStrs[1].toString())-1;
                    colArray[colArrayIndex] = Integer.parseInt(valStrs[2].toString());
                }
            }
            //计算乘积和
            int sum = 0;
            for (int i=0;i<matrix1Col;i++){
                sum = sum + rowArray[i]*colArray[i];
            }
            //输出结果矩阵中key坐标的值
            context.write(key,new IntWritable(sum));
        }

        /**
         * 根据坐标判断该点是否在结果矩阵中，减少不必要的计算
         * @param position
         * @return
         */
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
        //缺省combine
        job.setReducerClass(InnerProductReduce.class);
        //单独设置map输出，因为map和reduce输出不同，不能使用:job.setOutputKeyClass();
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
