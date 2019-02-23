import org.apache.commons.collections.KeyValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class InMemoryJoin extends Configured implements Tool {

    static HashMap<String, String> userMap = new HashMap<>();


    public static class Map extends Mapper<Text, Text, Text, Text> {
        private Text detailSet = new Text();
        String details;

        public void setup(Context context) throws IOException {
            // storing values from user data in hashmap
            Configuration configuration = context.getConfiguration();
            Path path = new Path(configuration.get("userData"));
            FileSystem fs = FileSystem.get(configuration);
            BufferedReader cache = new BufferedReader(new InputStreamReader(fs.open(path)));
            String input;
            while ((input = cache.readLine()) != null) {
                input = cache.readLine();
                if(input!=null){
                    String[] userDataArr = input.split(",");
                    String userId = userDataArr[0];
                    String requiredData = userDataArr[1] + ": " + userDataArr[5];
                    userMap.put(userId, requiredData);
                }
            }
        }

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            //processing larger dataset

            String[] usrlist = value.toString().split(",");

            if(userMap!=null && !userMap.isEmpty()){
                for(String usr : usrlist){
                    if (userMap.containsKey(usr)) {
                        details=userMap.get(usr);
                        userMap.remove(usr);
                        detailSet.set(details);
                        context.write(key, new Text(details));
                    }
                }
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
           Text res = new Text();
           StringBuilder sb = new StringBuilder();
           for (Text value : values){
                if(sb.length()==0){
                    sb.append("[");
                }
                sb.append(value.toString());
                sb.append(",");
            }

            sb.setLength(sb.length()-1);
            sb.append("]");
            res.set(sb.toString());
            context.write(key,res);

        }
    }


    public static void main(String args[]) throws Exception {
        int res = ToolRunner.run(new Configuration(), new InMemoryJoin(), args);
        System.exit(res);

    }

    public int run(String[] args) throws Exception {

        Configuration conf1 = new Configuration();
        if (args.length != 6) {
            System.err.println("Use: InMemoryJoin <userA> <userB> <inputFile> <outputDirectory>");
            System.exit(2);
        }

        conf1.set("userA", args[0]);
        conf1.set("userB", args[1]);
        Job job1 = new Job(conf1, "mutualFriends");
        job1.setJarByClass(InMemoryJoin.class);
        job1.setMapperClass(MutualFriends.Map.class);
        job1.setReducerClass(MutualFriends.Reduce.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[4]));
        FileOutputFormat.setOutputPath(job1, new Path(args[2]));


        int success = job1.waitForCompletion(true)?0:1;
        Configuration conf2 = new Configuration();
        conf2.set("userData", args[3]);
        @SuppressWarnings("deprication")
        Job job2 = new Job(conf2, "inMemoryJoin");
        job2.setJarByClass(InMemoryJoin.class);
        job2.setInputFormatClass(KeyValueTextInputFormat.class);
        job2.setMapperClass(Map.class);
        job2.setReducerClass(Reduce.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, new Path(args[2]));
        FileOutputFormat.setOutputPath(job2, new Path(args[5]));

        boolean success2 = job2.waitForCompletion(true);
        if (success2) {
            System.exit(0);
        } else System.exit(1);
        return  success;
    }
}
