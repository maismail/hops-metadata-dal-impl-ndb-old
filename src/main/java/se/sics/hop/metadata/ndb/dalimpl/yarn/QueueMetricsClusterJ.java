/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.hop.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.HopQueueMetrics;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.QueueMetricsDataAccess;
import se.sics.hop.metadata.yarn.tabledef.QueueMetricsTableDef;
import static se.sics.hop.metadata.yarn.tabledef.QueueMetricsTableDef.PENDING_VCORES;
import static se.sics.hop.metadata.yarn.tabledef.QueueMetricsTableDef.RESERVED_MB;

/**
 *
 * @author nickstanogias
 */
public class QueueMetricsClusterJ implements QueueMetricsTableDef, QueueMetricsDataAccess<HopQueueMetrics> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface QueueMetricsDTO {

    @PrimaryKey
    @Column(name = ID)
    String getid();

    void setid(String id);

    @Column(name = APPS_SUBMITTED)
    int getappssubmitted();

    void setappssubmitted(int appssubmitted);

    @Column(name = APPS_RUNNING)
    int getappsrunning();

    void setappsrunning(int appsrunning);

    @Column(name = APPS_PENDING)
    int getappspending();

    void setappspending(int appspending);

    @Column(name = APPS_COMPLETED)
    int getappscompleted();

    void setappscompleted(int appscompleted);

    @Column(name = APPS_KILLED)
    int getappskilled();

    void setappskilled(int appskilled);

    @Column(name = APPS_FAILED)
    int getappsfailed();

    void setappsfailed(int appsfailed);

    @Column(name = ALLOCATED_MB)
    int getallocatedmb();

    void setallocatedmb(int allocatedmb);

    @Column(name = ALLOCATED_VCORES)
    int getallocatedvcores();

    void setallocatedvcores(int allocatedvcores);

    @Column(name = ALLOCATED_CONTAINERS)
    int getallocatedcontainers();

    void setallocatedcontainers(int allocatedcontainers);

    @Column(name = AGGREGATE_CONTAINERS_ALLOCATED)
    long getaggregatecontainersallocated();

    void setaggregatecontainersallocated(long aggregatecontainersallocated);

    @Column(name = AGGREGATE_CONTAINERS_RELEASED)
    long getaggregatecontainersreleased();

    void setaggregatecontainersreleased(long aggregatecontainersreleased);

    @Column(name = AVAILABLE_MB)
    int getavailablemb();

    void setavailablemb(int availablemb);

    @Column(name = AVAILABLE_VCORES)
    int getavailablevcores();

    void setavailablevcores(int availablevcores);

    @Column(name = PENDING_MB)
    int getpendingmb();

    void setpendingmb(int pendingmb);

    @Column(name = PENDING_VCORES)
    int getpendingvcores();

    void setpendingvcores(int pendingvcores);

    @Column(name = PENDING_CONTAINERS)
    int getpendingContainers();

    void setpendingContainers(int pendingcontainers);
    
    @Column(name = RESERVED_MB)
    int getreservedmb();

    void setreservedmb(int reservedmb);

    @Column(name = RESERVED_VCORES)
    int getreservedvcores();

    void setreservedvcores(int reservedvcores);

    @Column(name = RESERVED_CONTAINERS)
    int getreservedcontainers();

    void setreservedcontainers(int reservedcontainers);

    @Column(name = ACTIVE_USERS)
    int getactiveusers();

    void setactiveusers(int activeusers);

    @Column(name = ACTIVE_APPLICATIONS)
    int getactiveapplications();

    void setactiveapplications(int activeapplications);

    @Column(name = PARENT_ID)
    int getparentid();

    void setparentid(int parentid);

    @Column(name = QUEUE_NAME)
    String getqueuename();

    void setqueuename(String queuename);

  }
  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public HopQueueMetrics findById(int id) throws StorageException {
    Session session = connector.obtainSession();

    QueueMetricsClusterJ.QueueMetricsDTO queueMetricsDTO = null;
    if (session != null) {
      queueMetricsDTO = session.find(QueueMetricsClusterJ.QueueMetricsDTO.class, id);
    }
    if (queueMetricsDTO == null) {
      throw new StorageException("HOP :: Error while retrieving row");
    }

    return createHopQueueMetrics(queueMetricsDTO);
  }

  @Override
  public List<HopQueueMetrics> findAll() throws StorageException, IOException {
    Session session = connector.obtainSession();
    QueryBuilder qb = session.getQueryBuilder();
    QueryDomainType<QueueMetricsClusterJ.QueueMetricsDTO> dobj = qb.createQueryDefinition(QueueMetricsClusterJ.QueueMetricsDTO.class);
    Query<QueueMetricsClusterJ.QueueMetricsDTO> query = session.createQuery(dobj);
    List<QueueMetricsClusterJ.QueueMetricsDTO> results = query.getResultList();
    session.flush();
    return createHopQueueMetricsList(results);

  }

  private List<HopQueueMetrics> createHopQueueMetricsList(List<QueueMetricsClusterJ.QueueMetricsDTO> list) throws IOException {
        List<HopQueueMetrics> queueMetricsList = new ArrayList<HopQueueMetrics>();
        for (QueueMetricsClusterJ.QueueMetricsDTO persistable : list) {
            queueMetricsList.add(createHopQueueMetrics(persistable));
        }
        return queueMetricsList;
    }
  
  @Override
  public void prepare(Collection<HopQueueMetrics> modified, Collection<HopQueueMetrics> removed) throws StorageException {
    Session session = connector.obtainSession();
    try {
      if (removed != null) {
        for (HopQueueMetrics hop : removed) {
          QueueMetricsClusterJ.QueueMetricsDTO persistable = session.newInstance(QueueMetricsClusterJ.QueueMetricsDTO.class, hop.getId());
          session.deletePersistent(persistable);
        }
      }
      if (modified != null) {
        for (HopQueueMetrics hop : modified) {
          QueueMetricsClusterJ.QueueMetricsDTO persistable = createPersistable(hop, session);
          session.savePersistent(persistable);
        }
      }
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  private HopQueueMetrics createHopQueueMetrics(QueueMetricsDTO queueMetricsDTO) {
    return new HopQueueMetrics(queueMetricsDTO.getid(),
            queueMetricsDTO.getappssubmitted(),
            queueMetricsDTO.getappsrunning(),
            queueMetricsDTO.getappspending(),
            queueMetricsDTO.getappscompleted(),
            queueMetricsDTO.getappskilled(),
            queueMetricsDTO.getappsfailed(),
            queueMetricsDTO.getallocatedmb(),
            queueMetricsDTO.getallocatedvcores(),
            queueMetricsDTO.getallocatedcontainers(),
            queueMetricsDTO.getaggregatecontainersallocated(),
            queueMetricsDTO.getaggregatecontainersreleased(),
            queueMetricsDTO.getavailablemb(),
            queueMetricsDTO.getavailablevcores(),
            queueMetricsDTO.getpendingmb(),
            queueMetricsDTO.getpendingvcores(),
            queueMetricsDTO.getpendingContainers(),
            queueMetricsDTO.getreservedmb(),
            queueMetricsDTO.getreservedvcores(),
            queueMetricsDTO.getreservedcontainers(),
            queueMetricsDTO.getactiveusers(),
            queueMetricsDTO.getactiveapplications(),
            queueMetricsDTO.getparentid(),
            queueMetricsDTO.getqueuename());
  }

  private QueueMetricsDTO createPersistable(HopQueueMetrics hop, Session session) {
    QueueMetricsClusterJ.QueueMetricsDTO queueMetricsDTO = session.newInstance(QueueMetricsClusterJ.QueueMetricsDTO.class);

    queueMetricsDTO.setactiveapplications(hop.getActiveapplications());
    queueMetricsDTO.setactiveusers(hop.getActiveusers());
    queueMetricsDTO.setaggregatecontainersallocated(hop.getAggregatecontainersallocated());
    queueMetricsDTO.setaggregatecontainersreleased(hop.getAggregatecontainersreleased());
    queueMetricsDTO.setallocatedcontainers(hop.getAllocatedcontainers());
    queueMetricsDTO.setallocatedmb(hop.getAllocatedmb());
    queueMetricsDTO.setallocatedvcores(hop.getAllocatedvcores());
    queueMetricsDTO.setappscompleted(hop.getAppscompleted());
    queueMetricsDTO.setappsfailed(hop.getAppsfailed());
    queueMetricsDTO.setappskilled(hop.getAppskilled());
    queueMetricsDTO.setappspending(hop.getAppspending());
    queueMetricsDTO.setappsrunning(hop.getAppsrunning());
    queueMetricsDTO.setappssubmitted(hop.getAppssubmitted());
    queueMetricsDTO.setavailablemb(hop.getAvailablemb());
    queueMetricsDTO.setavailablevcores(hop.getAvailablevcores());
    queueMetricsDTO.setid(hop.getId());
    queueMetricsDTO.setparentid(hop.getParentid());
    queueMetricsDTO.setpendingmb(hop.getPendingmb());
    queueMetricsDTO.setpendingvcores(hop.getPendingvcores());
    queueMetricsDTO.setqueuename(hop.getQueuename());
    queueMetricsDTO.setreservedcontainers(hop.getReservedcontainers());
    queueMetricsDTO.setreservedmb(hop.getReservedmb());
    queueMetricsDTO.setreservedvcores(hop.getReservedvcores());

    return queueMetricsDTO;
  }

}
