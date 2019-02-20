import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

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
import org.apache.hadoop.util.hash.Hash;


public class MutualFriends {
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        private Text userSet = new Text();


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] usrlist = value.toString().split("\t");

            if( usrlist.length == 1 ) {
                return;
            }
            String userId = usrlist[0];
            String friendsList = usrlist[1];
            String[] friendsId  = friendsList.split(",");
            for(String friend : friendsId){
                if(userId==friend)
                    continue;
                int userInt = Integer.parseInt(userId);
                int friendInt = Integer.parseInt(friend);
                if(userInt<friendInt){
                    userSet.set(userId+","+friend);
                }else{
                    userSet.set(friend+","+userId);
                }

                context.write(userSet, new Text(friendsList));
            }

        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        private Text mutualFriendCount = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashMap<String, Integer> friendsMap = new HashMap<String, Integer>();
            StringBuilder sb = new StringBuilder();
            for(Text friendsVal: values){
                String[] friendArr = friendsVal.toString().split(",");
                for(String friend : friendArr){
                    if(!friendsMap.containsKey(friend)){
                        friendsMap.put(friend,1);
                    }else{
                        sb.append(friend+",");
                    }

                }

            }
            if (sb.lastIndexOf(",") > -1) {
                sb.deleteCharAt(sb.lastIndexOf(","));
            }
            mutualFriendCount.set(new Text(sb.toString()));
            context.write(key,mutualFriendCount);
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "mutualFriends");
        job.setJarByClass(MutualFriends.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);
        System.out.println(success);
    }

}
