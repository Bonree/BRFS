package com.bonree.brfs.schedulers.task.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TaskResultModel {
    @JsonProperty("isSuccess")
    private boolean isSuccess = true;
    @JsonProperty("atoms")
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
