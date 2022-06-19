package io.substrait.relation;

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
}
