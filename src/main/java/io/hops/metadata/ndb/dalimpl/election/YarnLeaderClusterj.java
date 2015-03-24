package io.hops.metadata.ndb.dalimpl.election;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.metadata.election.TablesDef;
import io.hops.metadata.election.entity.LeDescriptor;

import java.security.InvalidParameterException;

public class YarnLeaderClusterj extends LeDescriptorClusterj
    implements TablesDef.YarnLeaderTableDef {

  @PersistenceCapable(table = TABLE_NAME)
  public interface YarnLeaderDTO extends LeaderDTO {

    @PrimaryKey
    @Column(name = TablesDef.LeDescriptorTableDef.ID)
    long getId();

    void setId(long id);

    @PrimaryKey
    @Column(name = TablesDef.LeDescriptorTableDef.PARTITION_VAL)
    int getPartitionVal();

    void setPartitionVal(int partitionVal);

    @Column(name = TablesDef.LeDescriptorTableDef.COUNTER)
    long getCounter();

    void setCounter(long counter);

    @Column(name = TablesDef.LeDescriptorTableDef.HOSTNAME)
    String getHostname();

    void setHostname(String hostname);

    @Column(name = TablesDef.LeDescriptorTableDef.HTTP_ADDRESS)
    String getHttpAddress();

    void setHttpAddress(String httpAddress);

  }

  @Override
  protected LeDescriptor createDescriptor(LeaderDTO lTable) {
    if (lTable.getPartitionVal() != 0) {
      throw new InvalidParameterException("Psrtition key should be zero");
    }
    return new LeDescriptor.YarnLeDescriptor(lTable.getId(),
        lTable.getCounter(), lTable.getHostname(), lTable.getHttpAddress());
  }

  public YarnLeaderClusterj() {
    super(YarnLeaderDTO.class);
  }
}
