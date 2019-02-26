import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.*;
import java.util.*;

public class ReduceSideJoin {
    public static class Map1 extends Mapper<LongWritable, Text, LongWritable, Text> {
        LongWritable user = new LongWritable();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] usrlist = value.toString().split("\t");
            if (usrlist.length == 1) {
                return;
            }
            String userId = usrlist[0];
            user.set(Long.parseLong(userId));
            String friendsList = usrlist[1];
            context.write(user, new Text("A" + friendsList));
        }
    }

    public static class Map2 extends Mapper<LongWritable, Text, LongWritable, Text> {
        Text res = new Text();
        LongWritable ageKey = new LongWritable();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split(",");
            Long userId = Long.parseLong(line[0]);
            if (line.length == 10) {
                //finding age
                String[] age = line[9].toString().split("/");
                Date date = new Date();
                Calendar cal = Calendar.getInstance();
                cal.setTime(cal.getTime());
                int year = cal.get(Calendar.YEAR);
                int month = cal.get(Calendar.MONTH);
                int day = cal.get(Calendar.DAY_OF_MONTH);
                int yearDiff = year - Integer.valueOf(age[2]);
                int monthDiff = month - Integer.valueOf(age[0]);
                int dayDiff = day - Integer.valueOf(age[2]);

                if (monthDiff == 0) {
                    if (dayDiff < 0) {
                        yearDiff--;
                    }
                } else if (monthDiff < 0) {
                    yearDiff--;
                }

                StringBuilder sb = new StringBuilder();
                sb.append(line[1]).append(",").append(new Integer(yearDiff).toString()).append(",").append(line[3]).append(",").append(line[4]).append(",").append(line[5]);
                res.set(sb.append("B").append(sb).toString());
                ageKey.set(userId);
                context.write(ageKey, res);
            }
        }
    }

    public static class Reduce1 extends Reducer<LongWritable, Text, Text, Text> {

        private ArrayList<Text> listA = new ArrayList<Text>();
        private ArrayList<Text> listB = new ArrayList<Text>();
        static HashMap<String, String> userMap = new HashMap<>();

        public void reduce(LongWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            Configuration configuration = context.getConfiguration();
            String path = configuration.get("userData");
            String userA = configuration.get("userA");
            String userB = configuration.get("userB");
            BufferedReader cache = new BufferedReader(new FileReader(path));
            String input;
            while ((input = cache.readLine()) != null) {
                input = cache.readLine();
                if (input != null) {
                    String[] userDataArr = input.split(",");
                    String userId = userDataArr[0];
                    String requiredData = userDataArr[1] + " : " + userDataArr[3] + " : " + userDataArr[9];
                    userMap.put(userId, requiredData);
                }
            }

            for (Text val : value) {
                if (val.toString().charAt(0) == 'A')
                    listA.add(new Text(val.toString().substring(2)));
                else if (val.toString().charAt(0) == 'B')
                    listB.add(new Text(val.toString().substring(2)));
            }

            String[] details = null;
            Text C = new Text();
            if (!listA.isEmpty() && !listB.isEmpty()) {
                for (Text A : listA) {
                    float agesum = 0;
                    int count = 0;
                    String[] friend = A.toString().split(",");
                    for (int i = 0; i < friend.length; i++) {
                        if (userMap.containsKey(friend[i])) {
                            String[] ageCal = userMap.get(friend[i]).split(":");
                            String[] ageIndx = ageCal[2].toString().split("/");
                            Date date = new Date();
                            Calendar cal = Calendar.getInstance();
                            cal.setTime(cal.getTime());
                            int year = cal.get(Calendar.YEAR);
                            int month = cal.get(Calendar.MONTH);
                            int day = cal.get(Calendar.DAY_OF_MONTH);
                            int yearDiff = year - Integer.valueOf(ageIndx[2]);
                            int monthDiff = month - Integer.valueOf(ageIndx[0]);
                            int dayDiff = day - Integer.valueOf(ageIndx[2]);


                            if (month < 0)
                                yearDiff--;
                            else if (month == 0) {
                                if (day < 0)
                                    yearDiff--;
                            }
                            agesum += yearDiff;
                            count++;
                        }

                    }
                    float avgAge = agesum / count;

//                    String subdetails="";
                    StringBuilder res = new StringBuilder();
                    for (Text B : listB) {
//                        details=B.toString().split(",");
//                        subdetails=B.toString()+","+new Text(new FloatWritable((float) avgAge).toString());
                        res.append(B.toString());
                        res.append(",");
                        res.append(new Text(new FloatWritable((float) avgAge).toString()));
                    }
                    C.set(res.toString());
                }
            }
            context.write(new Text(key.toString()), C);

        }
    }

    public static class Map3 extends Mapper<LongWritable, Text, CustomComparator, Text> {

    }

    public static class CustomComparator implements WritableComparable<CustomComparator> {

        private Integer user;
        private Integer friend;

        public CustomComparator() {
        }

        public CustomComparator(Integer user, Integer friend) {
            this.user = user;
            this.friend = friend;
        }

        public Integer getUser() {
            return user;
        }

        public void setUser(Integer user) {
            this.user = user;
        }

        public Integer getFriend() {
            return friend;
        }

        public void setFriend(Integer friend) {
            this.friend = friend;
        }


        @Override
        public int compareTo(CustomComparator o) {
            int res = user.compareTo(o.user);
            if(res!=0){
                return res;
            }
            return friend.compareTo(o.friend);
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeInt(user);
            dataOutput.writeInt(friend);

        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            user = dataInput.readInt();
            friend = dataInput.readInt();
        }

        @Override
        public String toString() {
            return "user=" + user.toString() +
                    ": friend=" + friend.toString() ;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null) {
                return false;
            }
            if (getClass() != o.getClass()) {
                return false;
            }
            final CustomComparator other = (CustomComparator) o;
            if (this.user != other.user && (this.user == null || !this.user.equals(other.user))) {
                return false;
            }
            if (this.friend != other.friend && (this.friend == null || !this.friend.equals(other.friend))) {
                return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            return Objects.hash(getUser(), getFriend());
        }
    }

    public class AgePartitioner extends Partitioner<CustomComparator, Text> {
        public int getPartition(CustomComparator friend, Text nullWritable, int numPartitions) {
            return friend.getFriend().hashCode() % numPartitions;
        }
    }

    public static class AgeBasicCompKeySortComparator extends WritableComparator {

        public AgeBasicCompKeySortComparator() {
            super(CustomComparator.class, true);
        }

        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            CustomComparator key1 = (CustomComparator) w1;
            CustomComparator key2 = (CustomComparator) w2;

            int cmpResult = -1 * (key1.getFriend().compareTo(key2.getFriend()));

            return cmpResult;
        }
    }

    public static class AgeBasicGroupingComparator extends WritableComparator {
        public AgeBasicGroupingComparator() {
            super(CustomComparator.class, true);
        }


        public int compare(WritableComparable w1, WritableComparable w2) {
            CustomComparator key1 = (CustomComparator) w1;
            CustomComparator key2 = (CustomComparator) w2;
            return -1 * (key1.getFriend().compareTo(key2.getFriend()));
        }
    }

    public static class Reducer2 extends Reducer<CustomComparator, Text, Text, Text> {
        TreeMap<String, String> output = new TreeMap<>();

        public void reduce(CustomComparator key, Iterable<Text> values, Context context) throws IOException, InterruptedException {


            for (Text val : values) {

                if (output.size() < 15) {
                    output.put(key.user.toString(), val.toString());
                    context.write(new Text(val.toString().split(",")[0]), new Text(val));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Path outputDirIntermediate1 = new Path(args[3]+ "_int1");
        Path outputDirIntermediate2 = new Path(args[4]+ "_int2");

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        conf.set("businessdata", otherArgs[0]);
        // get all args
        if (otherArgs.length != 5) {
            System.err.println("Usage: JoinExample <in> <in2> <in3> <out>");
            System.exit(2);
        }

        Job job = new Job(conf, "join1 ");
        job.setJarByClass(ReduceSideTemp.class);
        job.setReducerClass(ReduceSideTemp.HW1_4Reducer.class);

        MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class, ReduceSideJoin.Map1.class);
        MultipleInputs.addInputPath(job, new Path(otherArgs[2]), TextInputFormat.class, ReduceSideJoin.Map2.class);

        job.setOutputKeyClass(CustomComparator.class);
        job.setOutputValueClass(Text.class);


        FileOutputFormat.setOutputPath(job, outputDirIntermediate1);

        int code = job.waitForCompletion(true) ? 0 : 1;
        Job job1 = new Job(new Configuration(), "join2");
        job1.setJarByClass(ReduceSideJoin.class);


        FileInputFormat.addInputPath(job1, new Path(args[3] + "_int1"));

        job1.setMapOutputKeyClass(ReduceSideJoin.CustomComparator.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setPartitionerClass(ReduceSideJoin.AgePartitioner.class);
        job1.setMapperClass(ReduceSideJoin.Map3.class);

        job1.setSortComparatorClass(ReduceSideJoin.AgeBasicCompKeySortComparator.class);
        job1.setGroupingComparatorClass(ReduceSideJoin.AgeBasicGroupingComparator.class);
        job1.setReducerClass(ReduceSideJoin.Reducer2.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job1, outputDirIntermediate2);

        // Execute job and grab exit code
        code = job1.waitForCompletion(true) ? 0 : 1;

    }

}
