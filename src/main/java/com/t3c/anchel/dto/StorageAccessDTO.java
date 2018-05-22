package com.t3c.anchel.dto;

public class StorageAccessDTO {
	private String file;
	private Long key;
	private String folder;
	private String url;
	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getFile() {
		return file;
	}
	
	public void setFile(String file) {
		this.file = file;
	}
	public Long getKey() {
		return key;
	}
	public void setKey(Long key) {
		this.key = key;
	}
	public String getFolder() {
		return folder;
	}
	public void setFolder(String folder) {
		this.folder = folder;
	}

	@Override
	public String toString() {
		return "StorageAccessDTO [file=" + file + ", key=" + key + ", folder=" + folder + ", url=" + url + "]";
	}
}
