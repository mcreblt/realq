package io.realq.prototypes;

public interface Element {

	public void addInput();
	public void addArg();
	public void addOutput();
	public void setName();

}

class ElementImpl implements Element {

	@Override
	public void addInput() {
	}

	@Override
	public void addArg() {
	}

	@Override
	public void addOutput() {
	}

	@Override
	public void setName() {
	}
	
}

class ElementBuilderImpl extends ElementImpl implements Element,ElementBuilder<ElementStreamBuilder> {

	@Override
	public void setStreamBuilder(ElementStreamBuilder sb) {
		sb.setElement(this);
	}



}