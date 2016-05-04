package io.realq.prototypes;

import storm.trident.Stream;

public interface ElementStreamBuilder {

	void setElement(Element el);

	<T> T buildStream();
	
}

class ElementStreamBuilderImpl implements ElementStreamBuilder{
	Element el;
	
	@Override
	public void setElement(Element el) {
		this.el = el;
		
	}

	@Override
	public <T> T buildStream() {
		System.out.println("ElementStreamBuilderImpl with element: " + el.getClass());
		return null;
	}
	
}