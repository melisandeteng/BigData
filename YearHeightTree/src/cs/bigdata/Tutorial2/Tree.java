package cs.bigdata.Tutorial2;
// This is not useful for the exercise, but it said to create a class Tree so I did it anyway.
//Answer to the exercise is YearHeightTree.java
public class Tree {
	
	float[] geopoint;
	int arrondissement;
	String genre;
	String espece;
	String famille;
	int anneePlantation;
	float hauteur;
	float circonference;
	String adresse;
	String nomCommun;
	String variete;
	int objetID;
	String nomEv;
	
	// Default constructor
	public Tree(){
		
	}
	
	public Tree(float[] geopoint,
	int arrondissement,
	String genre,
	String espece,
	String famille,
	int anneePlantation,
	float hauteur,
	float circonference,
	String adresse,
	String nomCommun,
	String variete,
	int objetID,
	String nomEv){
		this.arrondissement = arrondissement;
		this.genre = genre;
		this.espece = espece;
		this.famille = famille;
		this.anneePlantation = anneePlantation;
		this.hauteur = hauteur;
		this.circonference = circonference;
		this.adresse = adresse;
		this.nomCommun = nomCommun;
		this.variete = variete;
		this.objetID = objetID;
		this.nomEv = nomEv;
	}

	public float getHauteur(){
		return this.hauteur;
	}
	
	public int getAnnee(){
		return this.anneePlantation;
	}
	
	public void setHauteur(float hauteur){
		this.hauteur = hauteur;
	}
	
	public void setAnnee(int anneePlantation){
		this.anneePlantation = anneePlantation;
	}
	
}
