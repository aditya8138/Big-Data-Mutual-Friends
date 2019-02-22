import org.apache.commons.collections.KeyValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class InMemoryJoin extends Configured implements Tool {

    static HashMap<String, String> userMap = new HashMap<>();

    public static class Map extends Mapper<IntWritable, Text, Text, Text> {
        private Text userIDSet = new Text();
        private Text detailSet = new Text();


        @Override
        protected void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException, FileNotFoundException {

            // storing values from user data in hashmap
            Configuration configuration = context.getConfiguration();
            String path = configuration.get("userData");

            BufferedReader cache = new BufferedReader(new FileReader(path));
            String input = "";
            String[] userDataArr;
            while ((input = cache.readLine()) != null) {
                input = cache.readLine();
                userDataArr = input.split(",");
                String userId = userDataArr[0];
                String requiredData = userDataArr[1] + ": " + userDataArr[5];
                userMap.put(userId, requiredData);
            }

            //processing larger dataset

            String[] usrlist = value.toString().split("\t");
            if (usrlist.length == 1) {
                return;
            }
            String userId = usrlist[0];
            String friendsList = usrlist[1];
            String[] friendsId = friendsList.split(",");
            StringBuilder sb = new StringBuilder("[");
            for (String friend : friendsId) {
                if (userMap.containsKey(friend)) {
                    sb.append(userMap.get(friend));
                    sb.append(",");
                }
            }
            sb.append("]");
            detailSet.set(sb.toString());
            context.write(userIDSet, detailSet);
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashMap<String, String> valMap = new HashMap<>();
            for (Text line : values) {
                String[] fields = line.toString().split("\t");
                if (fields.length == 1) {
                    return;
                }
                valMap.put(fields[0], fields[1]);


            }

            for (java.util.Map.Entry<String, String> mf : valMap.entrySet()) {
                context.write(new Text(mf.getKey()), new Text(mf.getValue()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new InMemoryJoin(), args);
        System.exit(res);
    }

    public int run (String[] args) throws Exception {
        Configuration conf1 = new Configuration();
        if (args.length != 6) {
            System.err.println("Use: InMemoryJoin <userA> <userB> <inputFile> <outputDirectory>");
            System.exit(2);
        }

        conf1.set("userA", args[0]);
        conf1.set("userB", args[1]);


        Job job = new Job(conf1, "mutualfriend");
        job.setJarByClass(InMemoryJoin.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(MutualFriends.Map.class);
        job.setReducerClass(MutualFriends.Reduce.class);
//        job.setInputFormatClass(TextInputFormat.class);
//        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[4]));
        Path path = new Path(args[2]);
        FileOutputFormat.setOutputPath(job, path);

        int success = job.waitForCompletion(true)?0:1;

        Configuration conf2 = getConf();
        conf2.set("userData", args[3]);
        Job job2 = new Job(conf2, "inMemoryJoin");
        job2.setJarByClass(InMemoryJoin.class);
        job2.setMapperClass(InMemoryJoin.Map.class);
        job2.setReducerClass(InMemoryJoin.Reduce.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job2, path);
        FileOutputFormat.setOutputPath(job2, new Path(args[5]));

        success = job.waitForCompletion(true)?0:1;


        return success;

    }
}
