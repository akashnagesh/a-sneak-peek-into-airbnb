package com.BigData.MapReduceAnalysis;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.BigData.utils.HBaseTablesName;
import com.me.Bigdata.CustomWritable.CustomKey;
import com.me.Bigdata.CustomWritable.GroupingComparator;

/**
 * I have used Top ten patterns here. Also used SequenceFileOutputFormat between
 * jobs.
 * 
 * @author akashnagesh
 *
 */
public class TopTen {

	public static void main(String[] args) throws Exception {

		if (args.length < 4) {
			System.out.println(
					"hadoop jar <jar location> <path to main class> <path to reviews.csv> <path for intermediate file> <path for final output> <cityname> ...");
			System.exit(0);
		}
		// TODO Auto-generated method stub
		try {
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "reviews-per-user");
			job.setJarByClass(TopTen.class);
			job.setMapperClass(ReviewsPerCustomerMapper.class);
			job.setReducerClass(ReviewsPerCustomerReducer.class);
			// job.setCombinerClass(ReducerWordCount.class);
			job.setOutputKeyClass(DoubleWritable.class);
			job.setOutputValueClass(IntWritable.class);

			job.setOutputFormatClass(SequenceFileOutputFormat.class);

			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));

			job.waitForCompletion(false);

			Configuration conf2 = new Configuration();
			Job job2 = Job.getInstance(conf2, "to-20-reviews");
			job2.setJarByClass(TopTen.class);
			job2.setMapperClass(TopTenMapper.class);
			job2.setReducerClass(TopTenReducer.class);
			// job.setCombinerClass(ReducerWordCount.class);
			job2.setOutputKeyClass(DoubleWritable.class);
			job2.setOutputValueClass(IntWritable.class);
			job2.setMapOutputKeyClass(CustomKey.class);
			job2.setMapOutputValueClass(NullWritable.class);
			job2.setInputFormatClass(SequenceFileInputFormat.class);
			job2.setGroupingComparatorClass(GroupingComparator.class);

			// job2.setOutputFormatClass(TextOutputFormat.class);

			FileInputFormat.addInputPath(job2, new Path(args[1]));
			FileOutputFormat.setOutputPath(job2, new Path(args[2]));

			job2.waitForCompletion(false);

			Configuration conf3 = HBaseConfiguration.create();
			conf3.set("Place", args[3]);
			Job job3 = Job.getInstance(conf3, "put-to-hbase");
			job3.setJarByClass(TopTen.class);
			job3.setMapperClass(IdentityMapper.class);
			job3.setMapOutputKeyClass(DoubleWritable.class);
			job3.setMapOutputValueClass(IntWritable.class);
			job3.setInputFormatClass(KeyValueTextInputFormat.class);

			FileInputFormat.addInputPath(job3, new Path(args[2]));
			TableMapReduceUtil.initTableReducerJob(HBaseTablesName.tableNameForAnalysisOfListingByPlace,
					TableWriterReducer.class, job3);
			job3.waitForCompletion(true);

			// cleanUpOutputDiectory(conf, args[1]);
			System.exit(0);
		} catch (InterruptedException e) {
			System.out.println(e);
		} catch (ClassNotFoundException e) {
			System.out.println(e);
		}
	}

	protected static class ReviewsPerCustomerMapper extends Mapper<Object, Text, DoubleWritable, IntWritable> {

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] split = value.toString().split("\t");

			if (split.length != 6)
				return;
			try {
				double reviewerId = Double.parseDouble(split[3]);
				context.write(new DoubleWritable(reviewerId), new IntWritable(1));
			} catch (Exception e) {
				System.out.println(e);
			}

		}
	}

	protected static class ReviewsPerCustomerReducer
			extends Reducer<DoubleWritable, IntWritable, DoubleWritable, IntWritable> {

		@Override
		protected void reduce(DoubleWritable keyIn, Iterable<IntWritable> valueIn, Context context)
				throws IOException, InterruptedException {
			int count = 0;
			for (IntWritable i : valueIn) {
				count += i.get();
			}
			context.write(keyIn, new IntWritable(count));
		}

	}

	protected static class TopTenMapper extends Mapper<DoubleWritable, IntWritable, CustomKey, NullWritable> {

		@Override
		protected void map(DoubleWritable key, IntWritable value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			try {
				context.write(new CustomKey(value.get(), key.get()), NullWritable.get());
			} catch (Exception e) {
				// TODO: handle exception
			}
		}

	}

	protected static class TopTenReducer extends Reducer<CustomKey, NullWritable, DoubleWritable, IntWritable> {
		static int numberOfUsers = 20;

		@Override
		protected void reduce(CustomKey arg0, Iterable<NullWritable> arg1, Context arg2)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			if (numberOfUsers <= 20) {
				arg2.write(new DoubleWritable(arg0.getId()), new IntWritable(arg0.getKey()));
			}
		}

	}

	protected static class IdentityMapper extends Mapper<Text, Text, DoubleWritable, IntWritable> {
		static {
			File f = new File("/Users/akashnagesh/Desktop/mapredSysout");
			try {
				System.setOut(new PrintStream(f));
			} catch (Exception e) {

			}
		}

		@Override
		protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			System.out.println("inside mapper");
			System.out.println(Double.parseDouble(key.toString()) + ":" + Integer.parseInt(value.toString()));
			context.write(new DoubleWritable(Double.parseDouble(key.toString())),
					new IntWritable(Integer.parseInt(value.toString())));
		}
	}

	protected static class TableWriterReducer
			extends TableReducer<DoubleWritable, IntWritable, ImmutableBytesWritable> {

		Connection connection;
		Admin admin;
		Table table;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			// place = context.getConfiguration().get("Place");

			connection = ConnectionFactory.createConnection(context.getConfiguration());
			// Get Admin
			admin = connection.getAdmin();

			if (admin.tableExists(TableName.valueOf(HBaseTablesName.tableNameForAnalysisOfListingByPlace))) {
				// Use the created table if its already exists
				// table =
				// connection.getTable(TableName.valueOf(HBaseTablesName.tableNameForAnalysisOfListingByPlace));
				table = connection.getTable(TableName.valueOf(HBaseTablesName.tableNameForAnalysisOfListingByPlace));

				if (!table.getTableDescriptor().hasFamily(Bytes.toBytes("TopTenCustomers"))) {

					// Create the Column family
					HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("TopTenCustomers");

					// Add the column family
					admin.addColumn(TableName.valueOf(HBaseTablesName.tableNameForAnalysisOfListingByPlace),
							hColumnDescriptor);

				}
			} else {

				// Create an Table Descriptor with table name
				HTableDescriptor tablefoThisJob = new HTableDescriptor(
						TableName.valueOf(HBaseTablesName.tableNameForAnalysisOfListingByPlace));

				// create an column descriptor for the table
				HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("TopTenCustomers");

				// add the column family for the table
				tablefoThisJob.addFamily(hColumnDescriptor);

				// This will create an new Table in the HBase
				admin.createTable(tablefoThisJob);
			}
		}

		@Override
		protected void reduce(DoubleWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			Put putToTable = new Put(Bytes.toBytes(context.getConfiguration().get("Place")));

			int value = values.iterator().next().get();
			// Adding Average price for Room type into hBase table
			putToTable.addColumn(Bytes.toBytes("TopTenCustomers"), Bytes.toBytes(key.get()), Bytes.toBytes(value));

			// Will Write to ListingsAnalyisByPlace HBase Table
			context.write(null, putToTable);

		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub

			// Close the Connection
			if (connection != null)
				connection.close();

			// Close the table Connection
			if (table != null)
				table.close();
		}
	}

}
