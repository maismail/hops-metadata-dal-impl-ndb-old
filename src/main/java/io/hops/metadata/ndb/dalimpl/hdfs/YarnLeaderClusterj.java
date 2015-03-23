package io.hops.metadata.ndb.dalimpl.hdfs;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.security.InvalidParameterException;

import io.hops.metadata.election.entity.LeDescriptor;
import io.hops.metadata.election.tabledef.LeDescriptorTableDef;
import io.hops.metadata.election.tabledef.YarnLeaderTableDef;

public class YarnLeaderClusterj extends LeDescriptorClusterj implements YarnLeaderTableDef {

  @PersistenceCapable(table = TABLE_NAME)
  public interface YarnLeaderDTO extends LeaderDTO{

    @PrimaryKey
    @Column(name = LeDescriptorTableDef.ID)
    long getId();

    void setId(long id);

    @PrimaryKey
    @Column(name = LeDescriptorTableDef.PARTITION_VAL)
    int getPartitionVal();

    void setPartitionVal(int partitionVal);

    @Column(name = LeDescriptorTableDef.COUNTER)
    long getCounter();

    void setCounter(long counter);

    @Column(name = LeDescriptorTableDef.HOSTNAME)
    String getHostname();

    void setHostname(String hostname);

    @Column(name = LeDescriptorTableDef.HTTP_ADDRESS)
    String getHttpAddress();

    void setHttpAddress(String httpAddress);

  }

  @Override
  protected LeDescriptor createDescriptor(LeaderDTO lTable) {
    if (lTable.getPartitionVal() != 0) {
      throw new InvalidParameterException("Psrtition key should be zero");
    }
    return new LeDescriptor.YarnLeDescriptor(lTable.getId(),
            lTable.getCounter(),
            lTable.getHostname(),
            lTable.getHttpAddress());
  }
   
    public YarnLeaderClusterj(){
        super(YarnLeaderDTO.class);
    }
}
