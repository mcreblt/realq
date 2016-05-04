package io.realq.parser.element;

import io.realq.parser.expr.Expr;

import java.util.ArrayList;
import java.util.List;

public class Element {

    private List<Expr> inputs = new ArrayList<Expr>();
    private List<Expr> outputs = new ArrayList<Expr>();
    private List<Expr> args = new ArrayList<Expr>();

    public void addOutput(Expr e) {
        if (!outputs.contains(e)) {
            outputs.add(e);
        }
    }

    public void addInput(Expr e) {
        if (!inputs.contains(e)) {
            inputs.add(e);
        }
    }

    public void addArg(Expr e) {
        args.add(e);
    }

    public List<Expr> getInputs() {
        return inputs;
    }

    public List<Expr> getOutputs() {
        return outputs;
    }

    public List<Expr> getArgs() {
        return args;
    }

}
