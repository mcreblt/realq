package io.realq.prototypes;

import storm.trident.TridentTopology;

public interface ElementAggrStreamBuilder<T> {

	void setElement(ElementAggr el);

	T buildStream();
}

class ElementAggrStreamBuilderTrident<Stream> implements ElementAggrStreamBuilder<Stream> {

	TridentTopology tridentTopology;
	ElementAggr el;
	
	public ElementAggrStreamBuilderTrident(TridentTopology tridentTopology) {
		super();
		this.tridentTopology = tridentTopology;
	}

	@Override
	public Stream buildStream() {
		System.out.println("ElementAggrStreamBuilderTrident with element: " + el.getClass());
		return null;
	}

	@Override
	public void setElement(ElementAggr el) {
		this.el = el;
		
	}

}