package com.bonree.brfs.duplication.filenode.duplicates;

import com.google.common.base.Strings;

public class DuplicateNode {
	private String group;
	private String id;
	/**
	 * 二级serverId
	 */
	private String secondId;
	
	public DuplicateNode() {
	}
	
	public DuplicateNode(String group, String id) {
		this.group = group;
		this.id = id;
	}

	public String getGroup() {
		return group;
	}

	public void setGroup(String group) {
		this.group = group;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getSecondId() {
		return secondId;
	}

	public DuplicateNode setSecondId(String secondId) {
		this.secondId = secondId;
		return this;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((group == null) ? 0 : group.hashCode());
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		result = prime*result+((secondId == null)? 0: secondId.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if(obj == null || !(obj instanceof DuplicateNode)) {
			return false;
		}
		
		DuplicateNode other = (DuplicateNode) obj;
		return Strings.nullToEmpty(group).equals(other.group)
				&& Strings.nullToEmpty(id).equals(other.id)&&Strings.nullToEmpty(secondId).equals(other.secondId);
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("{").append(group).append(", ").append(id).append(secondId).append("}");
		
		return builder.toString();
	}
}
