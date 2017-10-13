
package org.apache.hadoop.hbase.mapreduce.importtsv;

import java.util.TreeSet;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.Reducer;


public class MutilKeyValueSortReducer extends Reducer<ImmutableBytesWritable, KeyValue, ImmutableBytesWritable, KeyValue> {

  // The idea basically is to separate out each multi key individually. hence
  // using close
  protected void reduce(ImmutableBytesWritable rowAndTable, Iterable<KeyValue> kvs,
      Context context) throws java.io.IOException, InterruptedException {
    TreeSet<KeyValue> map = new TreeSet<KeyValue>(KeyValue.COMPARATOR);
    for (KeyValue kv: kvs) {
      map.add(kv.clone());
    }

    ImmutableBytesWritable tablePtr = new ImmutableBytesWritable(Utils.breakawayTableName(rowAndTable.copyBytes()));
    int index = 0;
    for (KeyValue kv: map) {
      context.write(tablePtr, kv);
    }
  }
}
