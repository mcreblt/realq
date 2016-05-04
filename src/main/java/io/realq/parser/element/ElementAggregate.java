package io.realq.parser.element;

import io.realq.parser.expr.Expr;

import java.util.ArrayList;
import java.util.List;

public class ElementAggregate extends ElementFunction {

    private List<Expr> group = new ArrayList<Expr>();

    private Long windowSize;
    private Long windowHop;

    // need to be one expr
    public void addGroupBy(Expr e) {
        if (!group.contains(e)) {
            group.add(e);
        }
    }

    public List<Expr> getGroupBy() {
        return group;
    }

    public Long getWindowSize() {
        return windowSize;
    }

    public void setWindowSize(Long windowSize) {
        this.windowSize = windowSize;
    }

    public Long getWindowHop() {
        return windowHop;
    }

    public void setWindowHop(Long windowHop) {
        this.windowHop = windowHop;
    }

}
