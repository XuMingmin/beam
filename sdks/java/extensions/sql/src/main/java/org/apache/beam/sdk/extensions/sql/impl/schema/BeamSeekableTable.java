package org.apache.beam.sdk.extensions.sql.impl.schema;

import java.io.Serializable;
import java.util.List;
import org.apache.beam.sdk.values.BeamRecord;

/**
 * A seekable table converts JOIN to inline lookup.
 */
public interface BeamSeekableTable extends Serializable{
  /**
   * return a list of {@code BeamRecord} with given key set.
   */
  List<BeamRecord> seekRecord(BeamRecord lookupSubRecord);
}
