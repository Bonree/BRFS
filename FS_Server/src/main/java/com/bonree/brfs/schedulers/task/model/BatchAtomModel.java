package com.bonree.brfs.schedulers.task.model;

import java.util.ArrayList;
import java.util.List;

public class BatchAtomModel {
	private List<AtomTaskModel> atoms = new ArrayList<AtomTaskModel>();

	public List<AtomTaskModel> getAtoms() {
		return atoms;
	}

	public void setAtoms(List<AtomTaskModel> atoms) {
		this.atoms = atoms;
	}
	public void addAll(List<AtomTaskModel> atoms) {
		this.atoms.addAll(atoms);
	}
	public void add(AtomTaskModel atom){
		this.atoms.add(atom);
	}
}
