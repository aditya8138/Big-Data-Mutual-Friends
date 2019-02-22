import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.*;

public class Top10Friends {
    public static class Map1 extends Mapper<LongWritable, Text, Text, Text> {
        private Text userSet = new Text();

        public void map(LongWritable key,Text value, Context context) throws IOException,InterruptedException {
            String[] usrlist = value.toString().split("\t");

            if( usrlist.length == 1 ) {
                return;
            }
            String userId = usrlist[0];
            String friendsList = usrlist[1];
            String[] friendsArr  = friendsList.split(",");

            for(String friend : friendsArr){
                if(Integer.valueOf(userId)<Integer.valueOf(friend)){
                    userSet.set(userId+","+friend);
                }else
                    userSet.set(friend+","+userId);
                context.write(userSet, new Text(friendsList));
            }
        }
    }

    public static class Reduce1 extends Reducer<Text, Text, Text, Text>{
        public  void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException{
            int count = 0;
            StringBuilder sb  = new StringBuilder();
            StringBuilder result  = new StringBuilder();
            HashMap<String, Integer> friendCount = new HashMap<>();
            for(Text friends : values){
                String[] friendsArr  =  friends.toString().split(",");
                for(String friend: friendsArr){
                    if(friendCount.containsKey(friend)){
                        count++;
                        sb.append(friend).append(",");
                    }else{
                        friendCount.put(friend,1);
                    }
                }

            }
            result.append(count).append("\t").append(sb);
            Text fCount= new Text(result.toString());
            context.write(key, fCount);

        }
    }

    public static class Map2 extends Mapper<LongWritable, Text, IntWritable, Text>{
        private final static IntWritable one = new IntWritable(1);
        public void map(LongWritable key, Text values, Context context) throws IOException,InterruptedException{
            context.write(one, values);
        }
    }

    public static  class  Reduce2 extends  Reducer<IntWritable, Text, Text, Text>{
        public void reduce(IntWritable key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException{
            // final reduce method to return top 10 mutual friends
            HashMap<String, String> mfMap = new HashMap<>();
            int count = 1;
            for(Text line : values){
                String[] fields  =line.toString().split("\t");
                if(fields.length==1){
                    return;
                }
                if(!fields[1].equals("0")){
                    mfMap.put(fields[0],fields[1]+"\t"+ fields[2]);
                }

            }

            Compare value = new Compare(mfMap);
            TreeMap<String,String> sorted = new TreeMap<>(value);
            sorted.putAll(mfMap);



           for (Map.Entry<String, String> mf : sorted.entrySet()) {

                if (count <= 10) {
                    context.write(new Text(mf.getKey()), new Text(mf.getValue().toString()));

                }
                else
                    break;
                count++;
            }
        }


    }

    public static class Compare implements Comparator<String>{
        HashMap<String, String> map;

        public Compare(HashMap<String, String> map){
            this.map=map;
        }

        public int compare(String val1, String val2){
            String[] mapVal1 = map.get(val1).split("\t");
            String[] mapVal2 = map.get(val2).split("\t");

            if(Integer.valueOf(mapVal1[0])<Integer.valueOf(mapVal2[0])){
                return 1;
            }else{
                return -1;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if(args.length!=3){
            System.err.println("not enough arguments");
            System.exit(2);
        }
        @SuppressWarnings("deprication")
        Job job1 = new Job(conf, "Top10Friends1");
        job1.setJarByClass(Top10Friends.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setMapperClass(Top10Friends.Map1.class);
        job1.setReducerClass(Top10Friends.Reduce1.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        job1.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        boolean success = job1.waitForCompletion(true);
        if(success){
            Configuration conf2 = new Configuration();
            @SuppressWarnings("deprication")
            Job job2 = new Job(conf, "Top10Friends2");
            job2.setJarByClass(Top10Friends.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(IntWritable.class);
            job2.setMapperClass(Top10Friends.Map2.class);
            job2.setReducerClass(Top10Friends.Reduce2.class);
            job2.setMapOutputKeyClass(IntWritable.class);
            job2.setMapOutputValueClass(Text.class);
            job2.setInputFormatClass(TextInputFormat.class);
            job2.setOutputFormatClass(TextOutputFormat.class);
            job2.setNumReduceTasks(1);

            FileInputFormat.setInputPaths(job2, new Path(args[1]));
            FileOutputFormat.setOutputPath(job2, new Path(args[2]));

            boolean success2 = job2.waitForCompletion(true);
            if(success2){
                System.exit(0);
            }else System.exit(1);
        }
    }
}
