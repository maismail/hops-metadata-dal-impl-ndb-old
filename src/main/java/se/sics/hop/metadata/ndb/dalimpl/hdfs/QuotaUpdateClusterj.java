package se.sics.hop.metadata.ndb.dalimpl.hdfs;

import com.mysql.clusterj.Query;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import com.mysql.clusterj.query.Predicate;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.dal.QuotaUpdateDataAccess;
import se.sics.hop.metadata.hdfs.entity.hop.QuotaUpdate;
import se.sics.hop.metadata.hdfs.tabledef.QuotaUpdateTableDef;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.ndb.mysqlserver.MysqlServerConnector;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import se.sics.hop.metadata.ndb.DBSession;

public class QuotaUpdateClusterj implements QuotaUpdateTableDef, QuotaUpdateDataAccess<QuotaUpdate> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface QuotaUpdateDTO {

    @PrimaryKey
    @Column(name = ID)
    int getId();

    void setId(int id);

    @PrimaryKey
    @Column(name = INODE_ID)
    int getInodeId();

    void setInodeId(int id);

    @Column(name = NAMESPACE_DELTA)
    long getNamespaceDelta();

    void setNamespaceDelta(long namespaceDelta);

    @Column(name = DISKSPACE_DELTA)
    long getDiskspaceDelta();

    void setDiskspaceDelta(long diskspaceDelta);
  }

  private ClusterjConnector connector = ClusterjConnector.getInstance();
  private MysqlServerConnector mysqlConnector = MysqlServerConnector.getInstance();

  @Override
  public void prepare(Collection<QuotaUpdate> added, Collection<QuotaUpdate> removed) throws StorageException {
    DBSession dbSession = connector.obtainSession();
    try {
      List<QuotaUpdateDTO> changes = new ArrayList<QuotaUpdateDTO>();
      List<QuotaUpdateDTO> deletions = new ArrayList<QuotaUpdateDTO>();
      if (removed != null) {
        for (QuotaUpdate update : removed) {
          QuotaUpdateDTO persistable = createPersistable(update, dbSession);
          deletions.add(persistable);
        }
      }
      if (added != null) {
        for (QuotaUpdate update : added) {
          QuotaUpdateDTO persistable = createPersistable(update, dbSession);
          changes.add(persistable);
        }
      }
      dbSession.getSession().deletePersistentAll(deletions);
      dbSession.getSession().savePersistentAll(changes);
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  private static final String FIND_QUERY = "SELECT * FROM " + TABLE_NAME + " ORDER BY " + ID + " LIMIT ";

  @Override
  public List<QuotaUpdate> findLimited(int limit) throws StorageException {
    ArrayList<QuotaUpdate> resultList;
    try {
      Connection conn = mysqlConnector.obtainSession();
      PreparedStatement s = conn.prepareStatement(FIND_QUERY + limit);
      ResultSet result = s.executeQuery();
      resultList = new ArrayList<QuotaUpdate>();

      while (result.next()) {
        int id = result.getInt(ID);
        int inodeId = result.getInt(INODE_ID);
        int namespaceDelta = result.getInt(NAMESPACE_DELTA);
        long diskspaceDelta = result.getLong(DISKSPACE_DELTA);
        resultList.add(new QuotaUpdate(id, inodeId, namespaceDelta, diskspaceDelta));
      }
    } catch (SQLException ex) {
      throw new StorageException(ex);
    }
    return resultList;
  }

  private QuotaUpdateDTO createPersistable(QuotaUpdate update, DBSession dbSession) {
    QuotaUpdateDTO dto = dbSession.getSession().newInstance(QuotaUpdateDTO.class);
    dto.setId(update.getId());
    dto.setInodeId(update.getInodeId());
    dto.setNamespaceDelta(update.getNamespaceDelta());
    dto.setDiskspaceDelta(update.getDiskspaceDelta());
    return dto;
  }

  private List<QuotaUpdate> createResultList(List<QuotaUpdateDTO> list) {
    List<QuotaUpdate> result = new ArrayList<QuotaUpdate>();
    for (QuotaUpdateDTO dto : list) {
      result.add(new QuotaUpdate(dto.getId(), dto.getInodeId(), dto.getNamespaceDelta(), dto.getDiskspaceDelta()));
    }
    return  result;
  }

  private static final String INODE_ID_PARAM = "inodeId";
  @Override
  public List<QuotaUpdate> findByInodeId(int inodeId) throws StorageException {

    try {
      DBSession dbSession = connector.obtainSession();

      QueryBuilder qb = dbSession.getSession().getQueryBuilder();
      QueryDomainType<QuotaUpdateDTO> dobj = qb.createQueryDefinition(QuotaUpdateDTO.class);
      Predicate pred1 = dobj.get("inodeId").equal(dobj.param(INODE_ID_PARAM));
      dobj.where(pred1);
      Query<QuotaUpdateDTO> query = dbSession.getSession().createQuery(dobj);
      query.setParameter(INODE_ID_PARAM, inodeId);

      List<QuotaUpdateDTO> results = query.getResultList();
      return createResultList(results);
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }
}