import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class Top10Friends {
    public static class Map1 extends Mapper<IntWritable, Text, Text, Text> {
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

    public static class reduce1 extends Reducer<Text, Text, Text, IntWritable>{
        private IntWritable count = new IntWritable(0);
        public  void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException{

        }
    }
}
