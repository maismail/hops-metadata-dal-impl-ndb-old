package io.hops.metadata.ndb.dalimpl.hdfs;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.dal.SafeBlocksDataAccess;
import io.hops.metadata.hdfs.tabledef.SafeBlocksTableDef;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.mysqlserver.HopsSQLExceptionHelper;
import io.hops.metadata.ndb.mysqlserver.MySQLQueryHelper;
import io.hops.metadata.ndb.mysqlserver.MysqlServerConnector;
import io.hops.metadata.ndb.wrapper.HopsSession;

public class SafeBlocksClusterj implements SafeBlocksTableDef, SafeBlocksDataAccess {
  
  @PersistenceCapable(table = TABLE_NAME)
  public interface SafeBlockDTO {
    
    @PrimaryKey
    @Column(name = ID)
    long getId();
    
    void setId(long id);
  }
  private ClusterjConnector connector = ClusterjConnector.getInstance();
  
  @Override
  public void insert(Collection<Long> safeBlocks) throws StorageException {
    final List<SafeBlockDTO> dtos = new ArrayList<SafeBlockDTO>(safeBlocks.size());
    final HopsSession session = connector.obtainSession();
    for (Long blk : safeBlocks) {
      SafeBlockDTO dto = create(session, blk);
      dtos.add(dto);
    }
    session.savePersistentAll(dtos);
  }
  
  @Override
  public void remove(Long safeBlock) throws StorageException {
    HopsSession session = connector.obtainSession();
    SafeBlockDTO dto = create(session, safeBlock);
    session.deletePersistent(dto);
  }


  @Override
  public int countAll() throws StorageException {
    return MySQLQueryHelper.countAll(TABLE_NAME);
  }

  @Override
  public void removeAll() throws StorageException {
    try {
      while (countAll() != 0) {
        MysqlServerConnector.truncateTable(TABLE_NAME, 10000);
      }
    } catch (SQLException ex) {
      throw HopsSQLExceptionHelper.wrap(ex);
    }
  }

  private SafeBlockDTO create(HopsSession session, Long blk)
      throws StorageException {
    SafeBlockDTO dto = session.newInstance(SafeBlockDTO.class);
    dto.setId(blk);
    return dto;
  }
}
