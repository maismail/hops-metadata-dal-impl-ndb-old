package se.sics.hop.metadata.ndb.dalimpl.hdfs;

import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.Index;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import com.mysql.clusterj.query.Predicate;
import com.mysql.clusterj.query.PredicateOperand;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import se.sics.hop.metadata.hdfs.dal.INodeDataAccess;
import se.sics.hop.metadata.hdfs.entity.hdfs.HopINode;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.ndb.mysqlserver.CountHelper;
import se.sics.hop.metadata.hdfs.tabledef.INodeTableDef;

/**
 *
 * @author Hooman <hooman@sics.se>
 * @author Salman <salman@sics.se>
 */
public class INodeClusterj implements INodeTableDef, INodeDataAccess<HopINode> {

  @Override
  public int countAll() throws StorageException {
    return CountHelper.countAll(TABLE_NAME);
  }

  @PersistenceCapable(table = TABLE_NAME)
  @Index(name = "path_lookup_idx")
  public interface InodeDTO {

    @PrimaryKey
    @Column(name = ID)
    long getId();     // id of the inode

    void setId(long id);

    @Column(name = NAME)
    String getName();     //name of the inode

    void setName(String name);

    //id of the parent inode 
    @Column(name = PARENT_ID)
    @Index(name = "parent_idx")
    long getParentId();     // id of the inode

    void setParentId(long parentid);

    // marker for InodeDirectory
    @Column(name = IS_DIR)
    int getIsDir();

    void setIsDir(int isDir);

    // marker for InodeDirectoryWithQuota
    @Column(name = IS_DIR_WITH_QUOTA)
    int getIsDirWithQuota();

    void setIsDirWithQuota(int isDirWithQuota);

    // Inode
    @Column(name = MODIFICATION_TIME)
    long getModificationTime();

    void setModificationTime(long modificationTime);

    // Inode
    @Column(name = ACCESS_TIME)
    long getATime();

    void setATime(long modificationTime);

    // Inode
    @Column(name = PERMISSION)
    byte[] getPermission();

    void setPermission(byte[] permission);

    //  marker for InodeFileUnderConstruction
    @Column(name = IS_UNDER_CONSTRUCTION)
    int getIsUnderConstruction();

    void setIsUnderConstruction(int isUnderConstruction);

    // InodeFileUnderConstruction
    @Column(name = CLIENT_NAME)
    String getClientName();

    void setClientName(String isUnderConstruction);

    // InodeFileUnderConstruction
    @Column(name = CLIENT_MACHINE)
    String getClientMachine();

    void setClientMachine(String clientMachine);

    @Column(name = CLIENT_NODE)
    String getClientNode();

    void setClientNode(String clientNode);

    //  marker for InodeFile
    @Column(name = IS_CLOSED_FILE)
    int getIsClosedFile();

    void setIsClosedFile(int isClosedFile);

    // InodeFile
    @Column(name = HEADER)
    long getHeader();

    void setHeader(long header);

    //INodeSymlink
    @Column(name = SYMLINK)
    String getSymlink();

    void setSymlink(String symlink);
  }
  private ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public void prepare(Collection<HopINode> removed, Collection<HopINode> newEntries, Collection<HopINode> modified) throws StorageException {
    Session session = connector.obtainSession();
    try {
      for (HopINode inode : removed) {
        InodeDTO persistable = session.newInstance(InodeDTO.class, inode.getId());
        session.deletePersistent(persistable);
      }
      session.flush();
      for (HopINode inode : newEntries) {
        InodeDTO persistable = session.newInstance(InodeDTO.class);
        createPersistable(inode, persistable);
        session.savePersistent(persistable);
      }

      for (HopINode inode : modified) {
        InodeDTO persistable = session.newInstance(InodeDTO.class);
        createPersistable(inode, persistable);
        session.savePersistent(persistable);
      }
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public HopINode findInodeById(long inodeId) throws StorageException {
    Session session = connector.obtainSession();
    try {
      InodeDTO persistable = session.find(InodeDTO.class, inodeId);

      if (persistable
              == null) {
        return null;
      }
      HopINode inode = createInode(persistable);

      return inode;
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public List<HopINode> findInodesByParentIdSortedByName(long parentId) throws StorageException {
    try {
      Session session = connector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();

      QueryDomainType<InodeDTO> dobj = qb.createQueryDefinition(InodeDTO.class);
      Predicate pred1 = dobj.get("parentId").equal(dobj.param("parentID"));
      dobj.where(pred1);
      Query<InodeDTO> query = session.createQuery(dobj);
      query.setParameter("parentID", parentId);

      List<InodeDTO> results = query.getResultList();
      return createInodeList(results);
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public HopINode findInodeByNameAndParentId(String name, long parentId) throws StorageException {
    try {
      Session session = connector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();

      QueryDomainType<InodeDTO> dobj = qb.createQueryDefinition(InodeDTO.class);

      Predicate pred1 = dobj.get("name").equal(dobj.param("name"));
      Predicate pred2 = dobj.get("parentId").equal(dobj.param("parentID"));

      dobj.where(pred1.and(pred2));
      Query<InodeDTO> query = session.createQuery(dobj);

      query.setParameter(
              "name", name);
      query.setParameter(
              "parentID", parentId);
      List<InodeDTO> results = query.getResultList();

      if (results.size() > 1) {
        throw new StorageException("This parent has two chidlren with the same name");
      } else if (results.isEmpty()) {
        return null;
      } else {
        return createInode(results.get(0));
      }
    } catch (Exception e) {
      throw new StorageException(e);
    }

  }

  @Override
  public List<HopINode> findInodesByIds(List<Long> ids) throws StorageException {
    try {
      Session session = connector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<InodeDTO> dobj = qb.createQueryDefinition(InodeDTO.class);
      PredicateOperand field = dobj.get("id");
      PredicateOperand values = dobj.param("param");
      Predicate predicate = field.in(values);
      dobj.where(predicate);
      Query<InodeDTO> query = session.createQuery(dobj);
      query.setParameter("param", ids.toArray());
      List<InodeDTO> results = query.getResultList();
      List<HopINode> inodes = null;
      return createInodeList(results);
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  private List<HopINode> createInodeList(List<InodeDTO> list) throws IOException {
    List<HopINode> inodes = new ArrayList<HopINode>();
    for (InodeDTO persistable : list) {
      inodes.add(createInode(persistable));
    }
    return inodes;
  }

  private HopINode createInode(InodeDTO persistable) throws IOException {
    return new HopINode(
            persistable.getId(),
            persistable.getName(),
            persistable.getParentId(),
            persistable.getIsDir(),
            persistable.getIsDirWithQuota(),
            persistable.getModificationTime(),
            persistable.getATime(),
            persistable.getPermission(),
            persistable.getIsUnderConstruction(),
            persistable.getClientName(),
            persistable.getClientMachine(),
            persistable.getClientNode(),
            persistable.getIsClosedFile(),
            persistable.getHeader(),
            persistable.getSymlink());
  }

  private void createPersistable(HopINode inode, InodeDTO persistable) {
    persistable.setId(inode.getId());
    persistable.setName(inode.getName());
    persistable.setParentId(inode.getParentId());
    persistable.setIsDir(inode.getIsDir());
    persistable.setIsDirWithQuota(inode.getIsDirWithQuota());
    persistable.setModificationTime(inode.getModificationTime());
    persistable.setATime(inode.getAccessTime());
    persistable.setPermission(inode.getPermission());
    persistable.setIsUnderConstruction(inode.getIsUnderConstruction());
    persistable.setClientName(inode.getClientName());
    persistable.setClientMachine(inode.getClientMachine());
    persistable.setClientNode(inode.getClientNode());
    persistable.setIsClosedFile(inode.getIsClosedFile());
    persistable.setHeader(inode.getHeader());
    persistable.setSymlink(inode.getSymlink());
  }
}
