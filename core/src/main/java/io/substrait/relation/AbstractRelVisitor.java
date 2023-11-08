package io.substrait.relation;

import io.substrait.relation.physical.HashJoin;
import io.substrait.relation.physical.MergeJoin;
import io.substrait.relation.physical.NestedLoopJoin;

public abstract class AbstractRelVisitor<OUTPUT, EXCEPTION extends Exception>
    implements RelVisitor<OUTPUT, EXCEPTION> {
  public abstract OUTPUT visitFallback(Rel rel);

  @Override
  public OUTPUT visit(Aggregate aggregate) throws EXCEPTION {
    return visitFallback(aggregate);
  }

  @Override
  public OUTPUT visit(EmptyScan emptyScan) throws EXCEPTION {
    return visitFallback(emptyScan);
  }

  @Override
  public OUTPUT visit(Fetch fetch) throws EXCEPTION {
    return visitFallback(fetch);
  }

  @Override
  public OUTPUT visit(Filter filter) throws EXCEPTION {
    return visitFallback(filter);
  }

  @Override
  public OUTPUT visit(Join join) throws EXCEPTION {
    return visitFallback(join);
  }

  @Override
  public OUTPUT visit(Set set) throws EXCEPTION {
    return visitFallback(set);
  }

  @Override
  public OUTPUT visit(NamedScan namedScan) throws EXCEPTION {
    return visitFallback(namedScan);
  }

  @Override
  public OUTPUT visit(LocalFiles localFiles) throws EXCEPTION {
    return visitFallback(localFiles);
  }

  @Override
  public OUTPUT visit(Project project) throws EXCEPTION {
    return visitFallback(project);
  }

  @Override
  public OUTPUT visit(Sort sort) throws EXCEPTION {
    return visitFallback(sort);
  }

  @Override
  public OUTPUT visit(VirtualTableScan virtualTableScan) throws EXCEPTION {
    return visitFallback(virtualTableScan);
  }

  @Override
  public OUTPUT visit(Cross cross) throws EXCEPTION {
    return visitFallback(cross);
  }

  @Override
  public OUTPUT visit(ExtensionLeaf extensionLeaf) throws EXCEPTION {
    return visitFallback(extensionLeaf);
  }

  @Override
  public OUTPUT visit(ExtensionSingle extensionSingle) throws EXCEPTION {
    return visitFallback(extensionSingle);
  }

  @Override
  public OUTPUT visit(ExtensionMulti extensionMulti) throws EXCEPTION {
    return visitFallback(extensionMulti);
  }

  @Override
  public OUTPUT visit(ExtensionTable extensionTable) throws EXCEPTION {
    return visitFallback(extensionTable);
  }

  @Override
  public OUTPUT visit(HashJoin hashJoin) throws EXCEPTION {
    return visitFallback(hashJoin);
  }

  @Override
  public OUTPUT visit(MergeJoin mergeJoin) throws EXCEPTION {
    return visitFallback(mergeJoin);
  }

  @Override
  public OUTPUT visit(NestedLoopJoin nestedLoopJoin) throws EXCEPTION {
    return visitFallback(nestedLoopJoin);
  }
}
