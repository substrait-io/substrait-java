package io.substrait.type.proto;

import io.substrait.TestBase;
import io.substrait.relation.Rel;
import io.substrait.relation.physical.AbstractExchangeRel;
import io.substrait.relation.physical.BroadcastExchange;
import io.substrait.relation.physical.MultiBucketExchange;
import io.substrait.relation.physical.RoundRobinExchange;
import io.substrait.relation.physical.ScatterExchange;
import io.substrait.relation.physical.SingleBucketExchange;
import io.substrait.relation.physical.TargetType;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class ExchangeRelRoundtripTest extends TestBase {

  final Rel baseTable =
      b.namedScan(
          Collections.singletonList("exchange_test_table"),
          Arrays.asList("id", "amount", "name", "status"),
          Arrays.asList(R.I64, R.FP64, R.STRING, R.BOOLEAN));

  @Test
  void broadcastExchange() {
    Rel exchange = BroadcastExchange.builder().input(baseTable).partitionCount(1).build();

    verifyRoundTrip(exchange);
  }

  @Test
  void roundRobinExchange() {
    Rel exchange =
        RoundRobinExchange.builder().input(baseTable).exact(true).partitionCount(1).build();

    verifyRoundTrip(exchange);
  }

  @Test
  void scatterExchange() {
    Rel exchange =
        ScatterExchange.builder()
            .input(baseTable)
            .addFields(b.fieldReference(baseTable, 0))
            .partitionCount(1)
            .build();

    verifyRoundTrip(exchange);
  }

  @Test
  void singleBucketExchange() {
    Rel exchange =
        SingleBucketExchange.builder()
            .input(baseTable)
            .partitionCount(1)
            .expression(b.fieldReference(baseTable, 0))
            .build();

    verifyRoundTrip(exchange);
  }

  @Test
  void multiBucketExchange() {
    Rel exchange =
        MultiBucketExchange.builder()
            .input(baseTable)
            .expression(b.fieldReference(baseTable, 0))
            .constrainedToCount(true)
            .partitionCount(1)
            .build();

    verifyRoundTrip(exchange);
  }

  @Test
  void exchangeWithTargets() {
    AbstractExchangeRel.ExchangeTarget target1 =
        AbstractExchangeRel.ExchangeTarget.builder()
            .partitionIds(Arrays.asList(0, 1))
            .type(TargetType.Uri.builder().uri("hdfs://example.com/data1").build())
            .build();

    AbstractExchangeRel.ExchangeTarget target2 =
        AbstractExchangeRel.ExchangeTarget.builder()
            .partitionIds(Arrays.asList(2, 3))
            .type(TargetType.Uri.builder().uri("hdfs://example.com/data2").build())
            .build();

    List<AbstractExchangeRel.ExchangeTarget> targets = Arrays.asList(target1, target2);

    Rel exchange =
        BroadcastExchange.builder().input(baseTable).targets(targets).partitionCount(1).build();

    verifyRoundTrip(exchange);
  }

  @Test
  void nestedExchangeRelations() {
    Rel innerExchange = BroadcastExchange.builder().input(baseTable).partitionCount(1).build();

    Rel outerExchange =
        RoundRobinExchange.builder().input(innerExchange).exact(false).partitionCount(1).build();

    verifyRoundTrip(outerExchange);
  }
}
