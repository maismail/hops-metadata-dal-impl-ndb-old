package se.sics.hop.metadata.ndb.dalimpl.hdfs;

import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import com.mysql.clusterj.query.Predicate;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.dal.BlockChecksumDataAccess;
import se.sics.hop.metadata.hdfs.entity.hop.BlockChecksum;
import se.sics.hop.metadata.hdfs.entity.hop.HopEncodingStatus;
import se.sics.hop.metadata.hdfs.tabledef.BlockChecksumTableDef;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.ndb.DBSession;
import se.sics.hop.metadata.ndb.mysqlserver.MysqlServerConnector;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class BlockChecksumClusterj implements BlockChecksumTableDef, BlockChecksumDataAccess<BlockChecksum> {

  static final Log LOG = LogFactory.getLog(BlockChecksumClusterj.class);

  private ClusterjConnector clusterjConnector = ClusterjConnector.getInstance();
  private MysqlServerConnector mysqlConnector = MysqlServerConnector.getInstance();

  @PersistenceCapable(table = TABLE_NAME)
  public interface BlockChecksumDto {

    @PrimaryKey
    @Column(name = INODE_ID)
    int getInodeId();

    void setInodeId(int inodeId);

    @PrimaryKey
    @Column(name = BLOCK_INDEX)
    int getBlockIndex();

    void setBlockIndex(int blockIndex);

    @Column(name = CHECKSUM)
    long getChecksum();

    void setChecksum(long checksum);
  }

  @Override
  public void add(BlockChecksum blockChecksum) throws StorageException {
    LOG.info("ADD " + blockChecksum.toString());
    Session session = clusterjConnector.obtainSession().getSession();
    BlockChecksumDto dto = session.newInstance(BlockChecksumDto.class);
    copyState(blockChecksum, dto);
    session.savePersistent(dto);
  }

  @Override
  public void update(BlockChecksum blockChecksum) throws StorageException {
    LOG.info("UPDATE " + blockChecksum.toString());
    Session session = clusterjConnector.obtainSession().getSession();
    BlockChecksumDto dto = session.newInstance(BlockChecksumDto.class);
    copyState(blockChecksum, dto);
    session.updatePersistent(dto);
  }

  @Override
  public void delete(BlockChecksum blockChecksum) throws StorageException {
    LOG.info("DELETE " + blockChecksum.toString());
    Session session = clusterjConnector.obtainSession().getSession();
    BlockChecksumDto dto = session.newInstance(BlockChecksumDto.class);
    copyState(blockChecksum, dto);
    session.deletePersistent(dto);
  }

  @Override
  public BlockChecksum find(int inodeId, int blockIndex) throws StorageException {
    try {
      Session session = clusterjConnector.obtainSession().getSession();
      BlockChecksumDto dto = session.find(BlockChecksumDto.class, new Object[]{inodeId, blockIndex});
      if (dto == null) {
        return null;
      }
      return createBlockChecksum(dto);
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public Collection<BlockChecksum> findAll(int inodeId) throws StorageException {
    try {
      DBSession dbSession = clusterjConnector.obtainSession();
      QueryBuilder qb = dbSession.getSession().getQueryBuilder();
      QueryDomainType<BlockChecksumDto> dobj = qb.createQueryDefinition(BlockChecksumDto.class);
      Predicate pred1 = dobj.get("inodeId").equal(dobj.param("iNodeParam"));
      dobj.where(pred1);
      Query<BlockChecksumDto> query = dbSession.getSession().createQuery(dobj);
      query.setParameter("iNodeParam", inodeId);
      return createBlockChecksumList(query.getResultList());
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public void deleteAll(int inodeId) throws StorageException {
    final String query = String.format("DELETE FROM block_checksum WHERE %s=%d");
    try {
      Connection conn = mysqlConnector.obtainSession();
      PreparedStatement s = conn.prepareStatement(query);
      s.executeQuery();
    } catch (SQLException ex) {
      throw new StorageException(ex);
    } finally {
      mysqlConnector.closeSession();
    }
  }

  private void copyState(BlockChecksum blockChecksum, BlockChecksumDto dto) {
    dto.setInodeId(blockChecksum.getInodeId());
    dto.setBlockIndex(blockChecksum.getBlockIndex());
    dto.setChecksum(blockChecksum.getChecksum());
  }

  private List<BlockChecksum> createBlockChecksumList(List<BlockChecksumDto> dtoList) {
    List<BlockChecksum> list = new ArrayList<BlockChecksum>();
    for (BlockChecksumDto dto : dtoList) {
      list.add(createBlockChecksum(dto));
    }
    return list;
  }

  private BlockChecksum createBlockChecksum(BlockChecksumDto dto) {
    if (dto == null) {
      return null;
    }

    return new BlockChecksum(dto.getInodeId(), dto.getBlockIndex(), dto.getChecksum());
  }
}
