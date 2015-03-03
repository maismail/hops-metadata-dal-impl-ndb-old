package se.sics.hop.metadata.ndb.dalimpl.hdfs;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.Index;
import com.mysql.clusterj.annotation.PartitionKey;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.INodeIdentifier;
import se.sics.hop.metadata.hdfs.dal.INodeDataAccess;
import se.sics.hop.metadata.hdfs.entity.hdfs.HopINode;
import se.sics.hop.metadata.hdfs.entity.hdfs.ProjectedINode;
import se.sics.hop.metadata.hdfs.tabledef.INodeTableDef;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.ndb.NdbBoolean;
import se.sics.hop.metadata.ndb.mysqlserver.HopsSQLExceptionHelper;
import se.sics.hop.metadata.ndb.mysqlserver.MySQLQueryHelper;
import se.sics.hop.metadata.ndb.mysqlserver.MysqlServerConnector;
import se.sics.hop.metadata.ndb.wrapper.HopsPredicate;
import se.sics.hop.metadata.ndb.wrapper.HopsQuery;
import se.sics.hop.metadata.ndb.wrapper.HopsQueryBuilder;
import se.sics.hop.metadata.ndb.wrapper.HopsQueryDomainType;
import se.sics.hop.metadata.ndb.wrapper.HopsSession;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 *
 * @author Hooman <hooman@sics.se>
 * @author Salman <salman@sics.se>
 */
public class INodeClusterj implements INodeTableDef, INodeDataAccess<HopINode> {

  @Override
  public int countAll() throws StorageException {
    return MySQLQueryHelper.countAll(TABLE_NAME);
  }

  @PersistenceCapable(table = TABLE_NAME)
  @PartitionKey(column=PARENT_ID)
  @Index(name = "inode_idx")
  public interface InodeDTO {
    @Column(name = ID)
    int getId();     // id of the inode
    void setId(int id);

    @PrimaryKey
    @Column(name = NAME)
    String getName();     //name of the inode
    void setName(String name);

    //id of the parent inode 
    @PrimaryKey
    @Column(name = PARENT_ID)
    int getParentId();     // id of the inode
    void setParentId(int parentid);

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
    @Column(name = GENERATION_STAMP)
    int getGenerationStamp();

    void setGenerationStamp(int generation_stamp);

    // InodeFile
    @Column(name = HEADER)
    long getHeader();

    void setHeader(long header);

    //INodeSymlink
    @Column(name = SYMLINK)
    String getSymlink();

    void setSymlink(String symlink);

    @Column(name = DIR)
    byte getDir();

    void setDir(byte dir);

    @Column(name = QUOTA_ENABLED)
    byte getQuotaEnabled();

    void setQuotaEnabled(byte quotaEnabled);

    @Column(name = UNDER_CONSTRUCTION)
    boolean getUnderConstruction();

    void setUnderConstruction(boolean underConstruction);

    @Column(name = SUBTREE_LOCKED)
    byte getSubtreeLocked();

    void setSubtreeLocked(byte locked);

    @Column(name = SUBTREE_LOCK_OWNER)
    long getSubtreeLockOwner();

    void setSubtreeLockOwner(long leaderId);
  }

  private ClusterjConnector connector = ClusterjConnector.getInstance();
  private MysqlServerConnector mysqlConnector = MysqlServerConnector.getInstance();
  private final static int NOT_FOUND_ROW = -1000;
  
  @Override
  public void prepare(Collection<HopINode> removed, Collection<HopINode> newEntries, Collection<HopINode> modified) throws StorageException {
    HopsSession session = connector.obtainSession();
    List<InodeDTO> changes = new ArrayList<InodeDTO>();
    List<InodeDTO> deletions = new ArrayList<InodeDTO>();
    for (HopINode inode : removed) {
      Object[] pk = new Object[2];
      pk[0] = inode.getParentId();
      pk[1] = inode.getName();
      InodeDTO persistable = session.newInstance(InodeDTO.class, pk);
      deletions.add(persistable);
    }

    for (HopINode inode : newEntries) {
      InodeDTO persistable = session.newInstance(InodeDTO.class);
      createPersistable(inode, persistable);
      changes.add(persistable);
    }

    for (HopINode inode : modified) {
      InodeDTO persistable = session.newInstance(InodeDTO.class);
      createPersistable(inode, persistable);
      changes.add(persistable);
    }
    session.deletePersistentAll(deletions);
    session.savePersistentAll(changes);
  }

  @Override
  public HopINode indexScanfindInodeById(int inodeId) throws StorageException {
    //System.out.println("*** pruneScanfindInodeById, Id "+inodeId);
    HopsSession session = connector.obtainSession();

    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<InodeDTO> dobj = qb.createQueryDefinition(InodeDTO.class);
    HopsPredicate pred1 = dobj.get("id").equal(dobj.param("idParam"));
    dobj.where(pred1);

    HopsQuery<InodeDTO> query = session.createQuery(dobj);
    query.setParameter("idParam", inodeId);

    List<InodeDTO> results = query.getResultList();
    explain(query);
    if (results.size() > 1){
      throw new StorageException("Only one record was expected");
    }
    if (results.size() == 1){
      return createInode(results.get(0));
    } else{
      return null;
    }
  }

  @Override
  public List<HopINode> indexScanFindInodesByParentId(int parentId) throws StorageException {
    //System.out.println("*** indexScanFindInodesByParentId ");
    HopsSession session = connector.obtainSession();

    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<InodeDTO> dobj = qb.createQueryDefinition(InodeDTO.class);
    HopsPredicate pred1 = dobj.get("parentId").equal(dobj.param("parentIDParam"));
    dobj.where(pred1);
    HopsQuery<InodeDTO> query = session.createQuery(dobj);
    query.setParameter("parentIDParam", parentId);

    List<InodeDTO> results = query.getResultList();
    explain(query);
    return createInodeList(results);
  }

  @Override
  public List<ProjectedINode> findInodesForSubtreeOperationsWithReadLock(int parentId) throws StorageException {
    final String query = String.format("SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s FROM %s WHERE %s=%d LOCK IN SHARE MODE",
        ID, NAME, PARENT_ID, PERMISSION, HEADER, DIR, SYMLINK, QUOTA_ENABLED, UNDER_CONSTRUCTION,
        SUBTREE_LOCKED, SUBTREE_LOCK_OWNER, TABLE_NAME, PARENT_ID, parentId);
    ArrayList<ProjectedINode> resultList;
    try {
      Connection conn = mysqlConnector.obtainSession();
      PreparedStatement s = conn.prepareStatement(query);
      ResultSet result = s.executeQuery();
      resultList = new ArrayList<ProjectedINode>();

      while (result.next()) {
        resultList.add(new ProjectedINode(
            result.getInt(ID),
            result.getInt(PARENT_ID),
            result.getString(NAME),
            result.getBytes(PERMISSION),
            result.getLong(HEADER),
            result.getString(SYMLINK) == null ? false : true,
            result.getBoolean(DIR),
            result.getBoolean(QUOTA_ENABLED),
            result.getBoolean(UNDER_CONSTRUCTION),
            result.getBoolean(SUBTREE_LOCKED),
            result.getLong(SUBTREE_LOCK_OWNER)));
      }
    } catch (SQLException ex) {
      throw HopsSQLExceptionHelper.wrap(ex);
    } finally {
      mysqlConnector.closeSession();
    }
    return resultList;
  }

  @Override
  public HopINode pkLookUpFindInodeByNameAndParentId(String name, int parentId) throws StorageException {
    HopsSession session = connector.obtainSession();

    Object[] pk = new Object[2];
    pk[0] = parentId;
    pk[1] = name;

    InodeDTO result = session.find(InodeDTO.class, pk);
    if(result != null){
      return createInode(result);
    }else{
      return null;
    }
  }

  @Override
  public List<HopINode> getINodesPkBatched(String[] names, int[] parentIds) throws StorageException {
    HopsSession session = connector.obtainSession();
    List<InodeDTO> dtos = new ArrayList<InodeDTO>();
    for (int i = 0; i < names.length; i++) {
      InodeDTO dto = session.newInstance(InodeDTO.class,
          new Object[]{parentIds[i], names[i]});
      dto.setId(NOT_FOUND_ROW);
      dto = session.load(dto);
      dtos.add(dto);
    }
    session.flush();
    return createInodeList(dtos);
  }
  
  @Override
  public List<INodeIdentifier> getAllINodeFiles(long startId, long endId) throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<InodeDTO> dobj = qb.createQueryDefinition(InodeDTO.class);
    HopsPredicate pred = dobj.get("dir").equal(dobj.param("isDirParam"));
    HopsPredicate pred2 = dobj.get("id").between(dobj.param("startId"), dobj.param("endId"));
    dobj.where(pred.and(pred2));
    HopsQuery<InodeDTO> query = session.createQuery(dobj);
    query.setParameter("isDirParam", NdbBoolean.convert(false));
    //FIXME: InodeId is integer
    //startId is inclusive and endId exclusive
    query.setParameter("startId", (int)startId);
    query.setParameter("endId", (int)(endId-1));
    List<InodeDTO> dtos = query.getResultList();
    List<INodeIdentifier> res = new ArrayList<INodeIdentifier>();
    for(InodeDTO dto : dtos){
      res.add(new INodeIdentifier(dto.getId(), dto.getParentId(), dto.getName()));
    }
    return res;
  }
  
  
  @Override
  public boolean haveFilesWithIdsBetween(long startId, long endId) throws StorageException {
    return MySQLQueryHelper.exists(TABLE_NAME, String.format("%s=0 and %s between %d and %d", DIR, ID, startId, (endId - 1)));
  }
  
  @Override
  public boolean haveFilesWithIdsGreaterThan(long id) throws StorageException {
     return MySQLQueryHelper.exists(TABLE_NAME, String.format("%s=0 and %s>%d", DIR, ID, id));
  }
  
  @Override
  public long getMinFileId() throws StorageException {
    return MySQLQueryHelper.minInt(TABLE_NAME, ID, String.format("%s=0", DIR));
  }

  @Override
  public long getMaxFileId() throws StorageException {
    return MySQLQueryHelper.maxInt(TABLE_NAME, ID, String.format("%s=0", DIR));
  }

  @Override
  public int countAllFiles() throws StorageException {
    return MySQLQueryHelper.countWithCriterion(TABLE_NAME, String.format("%s=0", DIR));
  }
  
  private List<HopINode> createInodeList(List<InodeDTO> list) {
    List<HopINode> inodes = new ArrayList<HopINode>();
    for (InodeDTO persistable : list) {
      if(persistable.getId() != NOT_FOUND_ROW){
        inodes.add(createInode(persistable));
      }
    }
    return inodes;
  }

  private HopINode createInode(InodeDTO persistable) {
    return new HopINode(
        persistable.getId(),
        persistable.getName(),
        persistable.getParentId(),
        NdbBoolean.convert(persistable.getDir()),
        NdbBoolean.convert(persistable.getQuotaEnabled()),
        persistable.getModificationTime(),
        persistable.getATime(),
        persistable.getPermission(),
        persistable.getUnderConstruction(),
        persistable.getClientName(),
        persistable.getClientMachine(),
        persistable.getClientNode(),
        persistable.getGenerationStamp(),
        persistable.getHeader(),
        persistable.getSymlink(),
        NdbBoolean.convert(persistable.getSubtreeLocked()),
        persistable.getSubtreeLockOwner());
  }

  private void createPersistable(HopINode inode, InodeDTO persistable) {
    persistable.setId(inode.getId());
    persistable.setName(inode.getName());
    persistable.setParentId(inode.getParentId());
    persistable.setDir((NdbBoolean.convert(inode.isDir())));
    persistable.setQuotaEnabled(NdbBoolean.convert(inode.isDirWithQuota()));
    persistable.setModificationTime(inode.getModificationTime());
    persistable.setATime(inode.getAccessTime());
    persistable.setPermission(inode.getPermission());
    persistable.setUnderConstruction(inode.isUnderConstruction());
    persistable.setClientName(inode.getClientName());
    persistable.setClientMachine(inode.getClientMachine());
    persistable.setClientNode(inode.getClientNode());
    persistable.setGenerationStamp(inode.getGenerationStamp());
    persistable.setHeader(inode.getHeader());
    persistable.setSymlink(inode.getSymlink());
    persistable.setSubtreeLocked(NdbBoolean.convert(inode.isSubtreeLocked()));
    persistable.setSubtreeLockOwner(inode.getSubtreeLockOwner());
  }

  private void explain(HopsQuery<InodeDTO> query){
//      Map<String,Object> map = query.explain();
//      System.out.println("Explain");
//      System.out.println("keys " +Arrays.toString(map.keySet().toArray()));
//      System.out.println("values "+ Arrays.toString(map.values().toArray()));
  }

}
