package se.sics.hop.metadata.ndb.dalimpl.hdfs;

import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.Index;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

  static final Log LOG = LogFactory.getLog(EncodingStatusClusterj.class);

  private ClusterjConnector clusterjConnector = ClusterjConnector.getInstance();
  private MysqlServerConnector mysqlConnector = MysqlServerConnector.getInstance();

  @PersistenceCapable(table = TABLE_NAME)
  public interface EncodingStatusDto {

    @PrimaryKey
    @Column(name = INODE_ID)
    long getInodeId();

    void setInodeId(long inodeId);

    @Column(name = STATUS)
    Integer getStatus();

    void setStatus(Integer status);

    @Column(name = CODEC)
    String getCodec();

    void setCodec(String codec);

    @Column(name = TARGET_REPLICATION)
    Integer getTargetReplication();

    void setTargetReplication(Integer targetReplication);

    @Column(name = STATUS_MODIFICATION_TIME)
    Long getStatusModificationTime();

    void setStatusModificationTime(Long modificationTime);

    @Index
    @Column(name = PARITY_INODE_ID)
    long getParityInodeId();

    // Long type not possible because of index
    void setParityInodeId(long inodeId);

    @Column(name = PARITY_STATUS)
    Integer getParityStatus();

    void setParityStatus(Integer status);

    @Column(name = PARITY_STATUS_MODIFICATION_TIME)
    Long getParityStatusModificationTime();

    void setParityStatusModificationTime(Long modificationTime);

    @Column(name = PARITY_FILE_NAME)
    String getParityFileName();

    void setParityFileName(String parityFileName);
  }

  @Override
  public void add(HopEncodingStatus status) {
    LOG.info("ADD " + status.toString());
    Session session = clusterjConnector.obtainSession();
    EncodingStatusDto dto = session.newInstance(EncodingStatusDto.class);
    copyState(status, dto);
    session.savePersistent(dto);
  }

  @Override
  public void update(HopEncodingStatus status) throws StorageException {
    LOG.info("UPDATE " + status.toString());
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
    Long inodeId = status.getInodeId();
    if (inodeId != null) {
      dto.setInodeId(inodeId);
    }
    Integer sourceStatus = status.getStatus();
    if (sourceStatus != null) {
      dto.setStatus(sourceStatus);
    }
    String codec = status.getCodec();
    if (codec != null) {
      dto.setCodec(codec);
    }
    Integer targetReplication = status.getTargetReplication();
    if (targetReplication != null) {
      dto.setTargetReplication(targetReplication);
    }
    Long statusModificationTime = status.getStatusModificationTime();
    if (statusModificationTime != null) {
      dto.setStatusModificationTime(statusModificationTime);
    }
    Long parityInodeId = status.getParityInodeId();
    if (parityInodeId != null) {
      dto.setParityInodeId(parityInodeId);
    }
    Integer parityStatus = status.getParityStatus();
    if (parityStatus != null) {
      dto.setParityStatus(parityStatus);
    }
    Long parityStatusModificationTime = status.getParityStatusModificationTime();
    if (parityStatusModificationTime != null) {
      dto.setParityStatusModificationTime(parityStatusModificationTime);
    }
    String parityFileName = status.getParityFileName();
    if (parityFileName != null) {
      dto.setParityFileName(parityFileName);
    }
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
  public HopEncodingStatus findByParityInodeId(long inodeId) throws StorageException {
    try {
      Session session = clusterjConnector.obtainSession();
      QueryBuilder builder = session.getQueryBuilder();
      QueryDomainType<EncodingStatusDto> domain = builder.createQueryDefinition(EncodingStatusDto.class);
      domain.where(domain.get("parityInodeId").equal(domain.param(PARITY_INODE_ID)));
      Query<EncodingStatusDto> query = session.createQuery(domain);
      query.setParameter(PARITY_INODE_ID, inodeId);

      List<EncodingStatusDto> results = query.getResultList();
      assert results.size() <= 1;

      if (results.size() == 0) {
        return null;
      }

      return createHopEncoding(results.get(0));
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public Collection<HopEncodingStatus> findRequestedEncodings(long limit) throws StorageException {
    return findWithStatus(HopEncodingStatus.ENCODING_REQUESTED, limit);
  }

  @Override
  public int countRequestedEncodings() throws StorageException {
    return CountHelper.countWhere(TABLE_NAME, STATUS + "=" + HopEncodingStatus.ENCODING_REQUESTED);
  }

  @Override
  public Collection<HopEncodingStatus> findRequestedRepairs(long limit) throws StorageException {
    return findWithStatus(HopEncodingStatus.REPAIR_REQUESTED, limit);
  }

  @Override
  public int countRequestedRepairs() throws StorageException {
    return CountHelper.countWhere(TABLE_NAME, STATUS + "=" + HopEncodingStatus.REPAIR_REQUESTED);
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

  @Override
  public Collection<HopEncodingStatus> findPotentiallyFixed(long limit) throws StorageException {
    return findWithStatus(HopEncodingStatus.POTENTIALLY_FIXED, limit);
  }

  @Override
  public int countPotentiallyFixed() throws StorageException {
    return CountHelper.countWhere(TABLE_NAME, STATUS + "=" + HopEncodingStatus.POTENTIALLY_FIXED);
  }

  @Override
  public Collection<HopEncodingStatus> findRequestedParityRepairs(long limit) throws StorageException {
    return findWithParityStatus(HopEncodingStatus.PARITY_REPAIR_REQUESTED, limit);
  }

  @Override
  public int countRequestedParityRepairs() throws StorageException {
    return CountHelper.countWhere(TABLE_NAME, PARITY_STATUS + "=" + HopEncodingStatus.PARITY_REPAIR_REQUESTED);
  }

  @Override
  public Collection<HopEncodingStatus> findActiveParityRepairs() throws StorageException {
    return findAllWithParityStatus(HopEncodingStatus.PARITY_REPAIR_ACTIVE);
  }

  @Override
  public int countActiveParityRepairs() throws StorageException {
    return CountHelper.countWhere(TABLE_NAME, PARITY_STATUS + "=" + HopEncodingStatus.PARITY_REPAIR_ACTIVE);
  }

  @Override
  public Collection<HopEncodingStatus> findPotentiallyFixedParities(long limit) throws StorageException {
    return findWithParityStatus(HopEncodingStatus.PARITY_POTENTIALLY_FIXED, limit);
  }

  @Override
  public int countPotentiallyFixedParities() throws StorageException {
    return CountHelper.countWhere(TABLE_NAME, PARITY_STATUS + "=" + HopEncodingStatus.PARITY_POTENTIALLY_FIXED);
  }

  private HopEncodingStatus createHopEncoding(EncodingStatusDto dto) {
    if (dto == null) {
      return null;
    }

    // This is necessary because Long cannot be used for clusterj fields with an index.
    // But it shouldn't be 0 anyway as it is always the root folder
    Long parityInodeId = null;
    if (dto.getParityInodeId() != 0) {
      parityInodeId = dto.getParityInodeId();
    }

    return new HopEncodingStatus(dto.getInodeId(), parityInodeId, dto.getStatus(), dto.getCodec(),
        dto.getTargetReplication(), dto.getStatusModificationTime(), dto.getParityStatus(),
        dto.getParityStatusModificationTime(), dto.getParityFileName());
  }

  private List<HopEncodingStatus> findAllWithStatus(int status) throws StorageException {
    return findWithStatus(status, Long.MAX_VALUE);
  }

  private List<HopEncodingStatus> findAllWithParityStatus(int status) throws StorageException {
    return findWithParityStatus(status, Long.MAX_VALUE);
  }

  private static final String LIMITED_STATUS_QUERY = "SELECT * FROM %s WHERE %s=%s ORDER BY %s ASC LIMIT %s";

  private List<HopEncodingStatus> findWithParityStatus(int findStatus, long limit) throws StorageException {
    return findStatus(PARITY_STATUS, findStatus, limit, PARITY_STATUS_MODIFICATION_TIME);
  }

  private List<HopEncodingStatus> findWithStatus(int findStatus, long limit) throws StorageException {
    return findStatus(STATUS, findStatus, limit, STATUS_MODIFICATION_TIME);
  }

  private List<HopEncodingStatus> findStatus(String column, int findStatus, long limit, String orderColumn)
      throws StorageException {
    String query = String.format(LIMITED_STATUS_QUERY, TABLE_NAME, column, findStatus, orderColumn, limit);
    ArrayList<HopEncodingStatus> resultList;
    try {
      Connection conn = mysqlConnector.obtainSession();
      PreparedStatement s = conn.prepareStatement(query);
      ResultSet result = s.executeQuery();

      resultList = new ArrayList<HopEncodingStatus>();

      while (result.next()) {
        Long inodeId = result.getLong(INODE_ID);
        Long parityInodeId = result.getLong(PARITY_INODE_ID);
        Integer status = result.getInt(STATUS);
        String codec = result.getString(CODEC);
        Integer targetReplication = result.getInt(TARGET_REPLICATION);
        Long statusModificationTime = result.getLong(STATUS_MODIFICATION_TIME);
        Integer parityStatus = result.getInt(PARITY_STATUS);
        Long parityStatusModificationTime = result.getLong(PARITY_STATUS_MODIFICATION_TIME);
        String parityFileName = result.getString(PARITY_FILE_NAME);
        resultList.add(new HopEncodingStatus(inodeId, parityInodeId, status, codec, targetReplication,
            statusModificationTime, parityStatus, parityStatusModificationTime, parityFileName));
      }
    } catch (SQLException ex) {
      throw new StorageException(ex);
    }

    return resultList;
  }
}