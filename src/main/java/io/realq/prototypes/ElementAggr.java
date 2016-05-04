package io.realq.prototypes;

public interface ElementAggr extends Element {

	public void addGroupBy();
	public void setWindowSize();
	public void setWindowHop();
}

class ElementAggrImpl extends ElementImpl implements ElementAggr{

	@Override
	public void addGroupBy() {
	}

	@Override
	public void setWindowSize() {
	}

	@Override
	public void setWindowHop() {
	}
	
}

class ElementAggrBuilderImpl<T> extends ElementAggrImpl implements ElementAggr,ElementBuilder<ElementAggrStreamBuilder<T>> {

	@Override
	public void setStreamBuilder(ElementAggrStreamBuilder<T> sb) {
		sb.setElement(this);
	}
}