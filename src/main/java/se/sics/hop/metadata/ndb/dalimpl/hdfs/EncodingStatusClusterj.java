package se.sics.hop.metadata.ndb.dalimpl.hdfs;

import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.dal.EncodingStatusDataAccess;
import se.sics.hop.metadata.hdfs.entity.hop.HopEncodingStatus;
import se.sics.hop.metadata.hdfs.tabledef.EncodingStatusTableDef;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.ndb.mysqlserver.CountHelper;
import se.sics.hop.metadata.ndb.mysqlserver.MysqlServerConnector;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class EncodingStatusClusterj implements EncodingStatusTableDef, EncodingStatusDataAccess<HopEncodingStatus> {

  private ClusterjConnector clusterjConnector = ClusterjConnector.getInstance();
  private MysqlServerConnector mysqlConnector = MysqlServerConnector.getInstance();

  @PersistenceCapable(table = TABLE_NAME)
  public interface EncodingStatusDto {

    @PrimaryKey
    @Column(name = INODE_ID)
    long getInodeId();

    void setInodeId(long inodeId);

    @Column(name = STATUS)
    int getStatus();

    void setStatus(int status);

    @Column(name = CODEC)
    String getCodec();

    void setCodec(String codec);

    @Column(name = MODIFICATION_TIME)
    long getModificationTime();

    void setModificationTime(long modificationTime);
  }

  @Override
  public void add(HopEncodingStatus status) {
    Session session = clusterjConnector.obtainSession();
    EncodingStatusDto dto = session.newInstance(EncodingStatusDto.class);
    copyState(status, dto);
    session.savePersistent(dto);
  }

  @Override
  public void update(HopEncodingStatus status) throws StorageException {
    Session session = clusterjConnector.obtainSession();
    EncodingStatusDto dto = session.newInstance(EncodingStatusDto.class);
    copyState(status, dto);
    session.updatePersistent(dto);
  }

  @Override
  public void delete(HopEncodingStatus status) throws StorageException {
    Session session = clusterjConnector.obtainSession();
    EncodingStatusDto dto = session.newInstance(EncodingStatusDto.class);
    copyState(status, dto);
    session.deletePersistent(dto);
  }

  private void copyState(HopEncodingStatus status, EncodingStatusDto dto) {
    dto.setInodeId(status.getInodeId());
    dto.setStatus(status.getStatus());
    dto.setCodec(status.getCodec());
    dto.setModificationTime(status.getModificationTime());
  }

  @Override
  public HopEncodingStatus findByInodeId(long inodeId) throws StorageException {
    try {
      Session session = clusterjConnector.obtainSession();
      EncodingStatusDto dto = session.find(EncodingStatusDto.class, inodeId);
      if (dto == null) {
        return null;
      }
      return createHopEncoding(dto);
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public Collection<HopEncodingStatus> findRequestedEncodings(long limit) throws StorageException {
    return findWithStatus(HopEncodingStatus.ENCODING_REQUESTED, limit);
  }

  @Override
  public Collection<HopEncodingStatus> findRequestedRepairs(long limit) throws StorageException {
    return findWithStatus(HopEncodingStatus.REPAIR_REQUESTED, limit);
  }

  @Override
  public int countRequestedEncodings() throws StorageException {
    return CountHelper.countWhere(TABLE_NAME, STATUS + "=" + HopEncodingStatus.ENCODING_REQUESTED);
  }

  @Override
  public Collection<HopEncodingStatus> findActiveEncodings() throws StorageException {
    return findAllWithStatus(HopEncodingStatus.ENCODING_ACTIVE);
  }

  @Override
  public int countActiveEncodings() throws StorageException {
    return CountHelper.countWhere(TABLE_NAME, STATUS + "=" + HopEncodingStatus.ENCODING_ACTIVE);
  }

  @Override
  public Collection<HopEncodingStatus> findEncoded(long limit) throws StorageException {
    return findWithStatus(HopEncodingStatus.ENCODED, limit);
  }

  @Override
  public int countEncoded() throws StorageException {
    return CountHelper.countWhere(TABLE_NAME, STATUS + "=" + HopEncodingStatus.ENCODED);
  }

  @Override
  public Collection<HopEncodingStatus> findActiveRepairs() throws StorageException {
    return findAllWithStatus(HopEncodingStatus.REPAIR_ACTIVE);
  }

  @Override
  public int countActiveRepairs() throws StorageException {
    return CountHelper.countWhere(TABLE_NAME, STATUS + "=" + HopEncodingStatus.REPAIR_ACTIVE);
  }

  private HopEncodingStatus createHopEncoding(EncodingStatusDto dto) {
    if (dto == null) {
      return null;
    }

    return new HopEncodingStatus(dto.getInodeId(), dto.getStatus(), dto.getCodec(), dto.getModificationTime());
  }

  private List<HopEncodingStatus> findAllWithStatus(int status) throws StorageException {
    return findWithStatus(status, Long.MAX_VALUE);
  }

  private static final String LIMITED_STATUS_QUERY = "SELECT * FROM %s WHERE %s=%s ORDER BY %s ASC LIMIT %s";

  private List<HopEncodingStatus> findWithStatus(int findStatus, long limit) throws StorageException {
    String query = String.format(LIMITED_STATUS_QUERY, TABLE_NAME, STATUS, findStatus, MODIFICATION_TIME, limit);
    ArrayList<HopEncodingStatus> resultList;
    try {
      Connection conn = mysqlConnector.obtainSession();
      PreparedStatement s = conn.prepareStatement(query);
      ResultSet result = s.executeQuery();

      resultList = new ArrayList<HopEncodingStatus>();

      while (result.next()) {
        long inodeId = result.getLong(INODE_ID);
        int status = result.getInt(STATUS);
        String codec = result.getString(CODEC);
        long modificationTime = result.getLong(MODIFICATION_TIME);
        resultList.add(new HopEncodingStatus(inodeId, status, codec, modificationTime));
      }
    } catch (SQLException ex) {
      throw new StorageException(ex);
    }

    return resultList;
  }
}
