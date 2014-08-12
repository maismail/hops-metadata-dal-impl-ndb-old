package se.sics.hop.metadata.ndb.dalimpl.hdfs;

import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.Index;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import com.mysql.clusterj.query.Predicate;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.INodeIdentifier;
import se.sics.hop.metadata.hdfs.dal.INodeDataAccess;
import se.sics.hop.metadata.hdfs.entity.hdfs.HopINode;
import se.sics.hop.metadata.hdfs.tabledef.INodeTableDef;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.ndb.mysqlserver.CountHelper;

import java.io.IOException;
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
    return CountHelper.countAll(TABLE_NAME);
  }

  @PersistenceCapable(table = TABLE_NAME)
  public interface InodeDTO {

    @Column(name = ID)
    @Index(name = "inode_idx")
    int getId();     // id of the inode
    void setId(int id);

    @PrimaryKey
    @Column(name = NAME)
    String getName();     //name of the inode
    void setName(String name);

    //id of the parent inode 
    @PrimaryKey
    @Column(name = PARENT_ID)
    @Index(name = "parent_idx")
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
    boolean getDir();

    void setDir(boolean dir);

    @Column(name = QUOTA_ENABLED)
    boolean getQuotaEnabled();

    void setQuotaEnabled(boolean quotaEnabled);

    @Column(name = UNDER_CONSTRUCTION)
    boolean getUnderConstruction();

    void setUnderConstruction(boolean underConstruction);

    @Column(name = SUBTREE_LOCKED)
    boolean getSubtreeLocked();

    void setSubtreeLocked(boolean locked);

    @Column(name = SUBTREE_LOCK_OWNER)
    long getSubtreeLockOwner();

    void setSubtreeLockOwner(long leaderId);
  }
  private ClusterjConnector connector = ClusterjConnector.getInstance();
  private final static int NOT_FOUND_ROW = -1000;
  
  @Override
  public void prepare(Collection<HopINode> removed, Collection<HopINode> newEntries, Collection<HopINode> modified) throws StorageException {
    Session session = connector.obtainSession();
    try {
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
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public HopINode indexScanfindInodeById(int inodeId) throws StorageException {
    try {
      //System.out.println("*** pruneScanfindInodeById, Id "+inodeId);
      Session session = connector.obtainSession();
      
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<InodeDTO> dobj = qb.createQueryDefinition(InodeDTO.class);
      Predicate pred1 = dobj.get("id").equal(dobj.param("idParam"));
      dobj.where(pred1);

      Query<InodeDTO> query = session.createQuery(dobj);
      query.setParameter("idParam", inodeId);
     
      List<InodeDTO> results = query.getResultList();
      explain(query);
      if(results.size() > 1){
        throw new StorageException("Only one record was expected");
      }
      if(results.size() == 1){
        return createInode(results.get(0));
      }else{
        return null;
      }
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public List<HopINode> indexScanFindInodesByParentId(int parentId) throws StorageException {
    try {
      //System.out.println("*** indexScanFindInodesByParentId ");
      Session session = connector.obtainSession();
            
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<InodeDTO> dobj = qb.createQueryDefinition(InodeDTO.class);
      Predicate pred1 = dobj.get("parentId").equal(dobj.param("parentIDParam"));
      dobj.where(pred1);
      Query<InodeDTO> query = session.createQuery(dobj);
      query.setParameter("parentIDParam", parentId);
      
      List<InodeDTO> results = query.getResultList();
      explain(query);
      return createInodeList(results);
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public HopINode pkLookUpFindInodeByNameAndParentId(String name, int parentId) throws StorageException {
    try {
     // System.out.println("*** pkLookUpFindInodeByNameAndParentId, name "+name+" parentId "+parentId);
      
      Session session = connector.obtainSession();

      Object[] pk = new Object[2];
      pk[0] = parentId;
      pk[1] = name;
      
      InodeDTO result = session.find(InodeDTO.class, pk);
      if(result != null){
        return createInode(result);
      }else{
        return null;
      }
    } catch (Exception e) {
      throw new StorageException(e);
    }

  }

  @Override
  public List<HopINode> getINodesPkBatched(String[] names, int[] parentIds) throws StorageException {
    try {
      Session session = connector.obtainSession();
      List<InodeDTO> dtos = new ArrayList<InodeDTO>();
      for (int i = 0; i < names.length; i++) {
        InodeDTO dto = session.newInstance(InodeDTO.class, new Object[]{parentIds[i], names[i]});
        dto.setId(NOT_FOUND_ROW);
        dto = session.load(dto);
        dtos.add(dto);
      }
      session.flush();
      return createInodeList(dtos);
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }
  
  @Override
  public List<INodeIdentifier> getAllINodeFiles() throws StorageException {
    try {
      Session session = connector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<InodeDTO> dobj = qb.createQueryDefinition(InodeDTO.class);
      Predicate pred = dobj.get("dir").equal(dobj.param("isDirParam"));
      dobj.where(pred);
      Query<InodeDTO> query = session.createQuery(dobj);
      query.setParameter("isDirParam", false);
      List<InodeDTO> dtos = query.getResultList();
      List<INodeIdentifier> res = new ArrayList<INodeIdentifier>();
      for(InodeDTO dto : dtos){
        res.add(new INodeIdentifier(dto.getId(), dto.getParentId(), dto.getName()));
      }
      return res;
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }
    
  private List<HopINode> createInodeList(List<InodeDTO> list) throws IOException {
    List<HopINode> inodes = new ArrayList<HopINode>();
    for (InodeDTO persistable : list) {
      if(persistable.getId() != NOT_FOUND_ROW){
        inodes.add(createInode(persistable));
      }
    }
    return inodes;
  }

  private HopINode createInode(InodeDTO persistable) throws IOException {
    return new HopINode(
        persistable.getId(),
        persistable.getName(),
        persistable.getParentId(),
        persistable.getDir(),
        persistable.getQuotaEnabled(),
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
        persistable.getSubtreeLocked(),
        persistable.getSubtreeLockOwner());
  }

  private void createPersistable(HopINode inode, InodeDTO persistable) {
    persistable.setId(inode.getId());
    persistable.setName(inode.getName());
    persistable.setParentId(inode.getParentId());
    persistable.setDir(inode.isDir());
    persistable.setQuotaEnabled(inode.isDirWithQuota());
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
  }

  private void explain(Query<InodeDTO> query){
//      Map<String,Object> map = query.explain();
//      System.out.println("Explain");
//      System.out.println("keys " +Arrays.toString(map.keySet().toArray()));
//      System.out.println("values "+ Arrays.toString(map.values().toArray()));
  }
}
