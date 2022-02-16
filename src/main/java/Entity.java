

public class Entity {

	private String id;

	protected String url="";
	protected String label="";

	public Entity(String url, String label) {
		this.url=url;
		this.label=label;
		this.id = label + "_" + url;
	}
	public Entity() {
		this.url = "";
		this.label="";
	}
	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}
	
	public String getLabel() {
		return label;
	}
	
	public void setLabel(String label) {
		this.label = label;
	}
	
	public String getId(){
		return this.id;
	}
	
	public void setId(String id) {
		this.id = id;
	}
}
