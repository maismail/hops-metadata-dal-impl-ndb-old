/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.hop.metadata.ndb.dalimpl.hdfs;

import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.dal.MisReplicatedRangeQueueDataAccess;
import se.sics.hop.metadata.hdfs.tabledef.MisReplicatedRangeQueueTableDef;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.ndb.mysqlserver.MySQLQueryHelper;

/**
 *
 * @author Mahmoud Ismail <maism@sics.se>
 */
public class MisReplicatedRangeQueueClusterj implements MisReplicatedRangeQueueTableDef, MisReplicatedRangeQueueDataAccess {

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
      Session session = connector.obtainSession();
      MisReplicatedRangeQueueDTO dto = session.newInstance(MisReplicatedRangeQueueDTO.class, getRange(start, end));
      session.savePersistent(dto);
    } catch (Exception e) {
      throw new StorageException(e);
    }

  }

  @Override
  public void remove(long start, long end) throws StorageException {
    try {
      Session session = connector.obtainSession();
      MisReplicatedRangeQueueDTO oldR = session.newInstance(MisReplicatedRangeQueueDTO.class, getRange(start, end));
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
