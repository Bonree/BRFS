package com.bonree.brfs.schedulers.task.model;

import java.util.ArrayList;
import java.util.List;

public class TaskResultModel {
    private boolean isSuccess = true;
    private List<AtomTaskResultModel> atoms = new ArrayList<AtomTaskResultModel>();

    public List<AtomTaskResultModel> getAtoms() {
        return atoms;
    }

    public void setAtoms(List<AtomTaskResultModel> atoms) {
        this.atoms = atoms;
    }

    public void addAll(List<AtomTaskResultModel> atoms) {
        this.atoms.addAll(atoms);
    }

    public void add(AtomTaskResultModel atom) {
        this.atoms.add(atom);
    }

    public boolean isSuccess() {
        return isSuccess;
    }

    public void setSuccess(boolean isSuccess) {
        this.isSuccess = isSuccess;
    }
}
