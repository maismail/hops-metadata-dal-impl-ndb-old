/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.hop.metadata.ndb.dalimpl.hdfs;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.dal.SafeBlocksDataAccess;
import se.sics.hop.metadata.hdfs.tabledef.SafeBlocksTableDef;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.ndb.mysqlserver.HopsSQLExceptionHelper;
import se.sics.hop.metadata.ndb.mysqlserver.MySQLQueryHelper;
import se.sics.hop.metadata.ndb.mysqlserver.MysqlServerConnector;
import se.sics.hop.metadata.ndb.wrapper.HopsSession;

/**
 *
 * @author Mahmoud Ismail <maism@sics.se>
 */
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
