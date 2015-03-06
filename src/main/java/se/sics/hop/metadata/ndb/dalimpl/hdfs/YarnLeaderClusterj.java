/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.hop.metadata.ndb.dalimpl.hdfs;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.security.InvalidParameterException;
import se.sics.hop.metadata.hdfs.entity.hop.election.LeDescriptor;
import se.sics.hop.metadata.hdfs.entity.hop.election.LeDescriptor.YarnLeDescriptor;
import se.sics.hop.metadata.hdfs.tabledef.YarnLeaderTableDef;

/**
 *
 * @author gautier
 */
public class YarnLeaderClusterj extends LeDescriptorClusterj implements YarnLeaderTableDef {

  @PersistenceCapable(table = TABLE_NAME)
  public interface YarnLeaderDTO extends LeaderDTO{

    @PrimaryKey
    @Column(name = ID)
    long getId();

    void setId(long id);

    @PrimaryKey
    @Column(name = PARTITION_VAL)
    int getPartitionVal();

    void setPartitionVal(int partitionVal);

    @Column(name = COUNTER)
    long getCounter();

    void setCounter(long counter);

    @Column(name = HOSTNAME)
    String getHostname();

    void setHostname(String hostname);

    @Column(name = HTTP_ADDRESS)
    String getHttpAddress();

    void setHttpAddress(String httpAddress);

  }

  @Override
  protected LeDescriptor createDescriptor(LeaderDTO lTable) {
    if (lTable.getPartitionVal() != 0) {
      throw new InvalidParameterException("Psrtition key should be zero");
    }
    return new YarnLeDescriptor(lTable.getId(),
            lTable.getCounter(),
            lTable.getHostname(),
            lTable.getHttpAddress());
  }
   
    public YarnLeaderClusterj(){
        super(YarnLeaderDTO.class);
    }
}
