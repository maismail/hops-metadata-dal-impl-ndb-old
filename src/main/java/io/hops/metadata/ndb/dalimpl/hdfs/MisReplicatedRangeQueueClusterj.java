package io.hops.metadata.ndb.dalimpl.hdfs;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.dal.MisReplicatedRangeQueueDataAccess;
import io.hops.metadata.hdfs.tabledef.MisReplicatedRangeQueueTableDef;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.mysqlserver.MySQLQueryHelper;
import io.hops.metadata.ndb.wrapper.HopsSession;

public class MisReplicatedRangeQueueClusterj
    implements MisReplicatedRangeQueueTableDef,
    MisReplicatedRangeQueueDataAccess {

  @PersistenceCapable(table = TABLE_NAME)
  public interface MisReplicatedRangeQueueDTO {

    @PrimaryKey
    @Column(name = RANGE)
    String getRange();

    void setRange(String range);
  }

  private ClusterjConnector connector = ClusterjConnector.getInstance();
  private final static String SEPERATOR = "-";

  @Override
  public void insert(long start, long end) throws StorageException {
    try {
      HopsSession session = connector.obtainSession();
      MisReplicatedRangeQueueDTO dto = session
          .newInstance(MisReplicatedRangeQueueDTO.class, getRange(start, end));
      session.savePersistent(dto);
    } catch (Exception e) {
      throw new StorageException(e);
    }

  }

  @Override
  public void remove(long start, long end) throws StorageException {
    try {
      HopsSession session = connector.obtainSession();
      MisReplicatedRangeQueueDTO oldR = session
          .newInstance(MisReplicatedRangeQueueDTO.class, getRange(start, end));
      session.deletePersistent(oldR);
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public int countAll() throws StorageException {
    return MySQLQueryHelper.countAll(TABLE_NAME);
  }

  private String getRange(long start, long end) {
    return start + SEPERATOR + end;
  }
}
