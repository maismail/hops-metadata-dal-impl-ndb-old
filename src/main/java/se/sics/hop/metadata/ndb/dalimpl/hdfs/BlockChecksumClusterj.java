package se.sics.hop.metadata.ndb.dalimpl.hdfs;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.dal.BlockChecksumDataAccess;
import se.sics.hop.metadata.hdfs.entity.hop.BlockChecksum;
import se.sics.hop.metadata.hdfs.tabledef.BlockChecksumTableDef;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.ndb.mysqlserver.HopsSQLExceptionHelper;
import se.sics.hop.metadata.ndb.mysqlserver.MysqlServerConnector;
import se.sics.hop.metadata.ndb.wrapper.HopsPredicate;
import se.sics.hop.metadata.ndb.wrapper.HopsQuery;
import se.sics.hop.metadata.ndb.wrapper.HopsQueryBuilder;
import se.sics.hop.metadata.ndb.wrapper.HopsQueryDomainType;
import se.sics.hop.metadata.ndb.wrapper.HopsSession;

import java.sql.Connection;
import java.sql.PreparedStatement;
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
    HopsSession session = clusterjConnector.obtainSession();
    BlockChecksumDto dto = session.newInstance(BlockChecksumDto.class);
    copyState(blockChecksum, dto);
    session.savePersistent(dto);
  }

  @Override
  public void update(BlockChecksum blockChecksum) throws StorageException {
    LOG.info("UPDATE " + blockChecksum.toString());
    HopsSession session = clusterjConnector.obtainSession();
    BlockChecksumDto dto = session.newInstance(BlockChecksumDto.class);
    copyState(blockChecksum, dto);
    session.updatePersistent(dto);
  }

  @Override
  public void delete(BlockChecksum blockChecksum) throws StorageException {
    LOG.info("DELETE " + blockChecksum.toString());
    HopsSession session = clusterjConnector.obtainSession();
    BlockChecksumDto dto = session.newInstance(BlockChecksumDto.class);
    copyState(blockChecksum, dto);
    session.deletePersistent(dto);
  }

  @Override
  public BlockChecksum find(int inodeId, int blockIndex) throws StorageException {
    HopsSession session = clusterjConnector.obtainSession();
    BlockChecksumDto dto =
        session.find(BlockChecksumDto.class, new Object[]{inodeId, blockIndex});
    if (dto == null) {
      return null;
    }
    return createBlockChecksum(dto);
  }

  @Override
  public Collection<BlockChecksum> findAll(int inodeId) throws StorageException {
    HopsSession session = clusterjConnector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<BlockChecksumDto> dobj =
        qb.createQueryDefinition(BlockChecksumDto.class);
    HopsPredicate pred1 = dobj.get("inodeId").equal(dobj.param("iNodeParam"));
    dobj.where(pred1);
    HopsQuery<BlockChecksumDto> query = session.createQuery(dobj);
    query.setParameter("iNodeParam", inodeId);
    return createBlockChecksumList(query.getResultList());
  }

  @Override
  public void deleteAll(int inodeId) throws StorageException {
    final String query = String.format("DELETE FROM block_checksum WHERE %s=%d");
    try {
      Connection conn = mysqlConnector.obtainSession();
      PreparedStatement s = conn.prepareStatement(query);
      s.executeQuery();
    } catch (SQLException ex) {
      throw HopsSQLExceptionHelper.wrap(ex);
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
