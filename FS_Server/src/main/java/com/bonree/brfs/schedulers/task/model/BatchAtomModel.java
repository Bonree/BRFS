package com.bonree.brfs.schedulers.task.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.ArrayList;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class BatchAtomModel {
    private ArrayList<AtomTaskModel> atoms = new ArrayList<AtomTaskModel>();

    public List<AtomTaskModel> getAtoms() {
        return this.atoms;
    }

    public void setAtoms(ArrayList<AtomTaskModel> atoms) {
        this.atoms = atoms;
    }

    public void addAll(List<AtomTaskModel> atoms) {
        this.atoms.addAll(atoms);
    }

    public void add(AtomTaskModel atom) {
        this.atoms.add(atom);
    }
}
