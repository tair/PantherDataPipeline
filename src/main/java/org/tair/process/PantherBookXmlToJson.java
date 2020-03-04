package org.tair.process;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.tair.module.Annotation;
import org.tair.module.Children;
import org.tair.module.PantherData;
import org.tair.util.Util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PantherBookXmlToJson {

	PantherData pantherData = null;
	private List<Annotation> annotations = null;

	public PantherData readPantherXmlToObject(String book_id) throws Exception {

		String url = "http://pantherdb.org/tempFamilySearch?type=book_info&book=" + book_id;
		String jsonString = Util.readContentFromWebUrlToJson(PantherData.class, url);

		// convert json string to Panther object
		this.pantherData = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT).readValue(jsonString,
				PantherData.class);
		this.pantherData.setId(book_id);
		return this.pantherData;
	}

	public PantherData readPantherTreeById(String family_id) throws Exception {
		String prunedTreeUrl = "http://pantherdb.org/tempFamilySearch?type=book_info&book=" + family_id
				+ "&taxonFltr=13333,3702,15368,51351,3055,2711,3659,4155,3847,3635,4232,112509,3880,214687,4097,39947,70448,42345,3218,3694,3760,3988,4555,4081,4558,3641,4565,29760,4577,29655,6239,7955,44689,7227,83333,9606,10090,10116,559292,284812";
		String jsonString = Util.readContentFromWebUrlToJson(PantherData.class, prunedTreeUrl);
		// convert json string to Panther object
		this.pantherData = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT).readValue(jsonString,
				PantherData.class);
		this.pantherData.setId(family_id);
		this.pantherData.setJsonString(jsonString);
		return this.pantherData;
	}

	private void flattenTree(Children children) {

		if (children == null || children.getAnnotation_node() == null || children.getAnnotation_node().size() == 0)
			return;

		for (Annotation annotation : children.getAnnotation_node()) {
			this.annotations.add(annotation);
			flattenTree(annotation.getChildren());
		}
	}

	private void buildListItems() {

		for (Annotation annotation : this.annotations) {
			// System.out.println(annotation.getAccession());
			addToListFromAnnotation(annotation);
		}
	}

	//PantherData has a list of unique variables which solr uses to index and search from and return the panther ID matching the search
	private void addToListFromAnnotation(Annotation annotation) {
		//Internal Node Variables
		//"accession"
		if (annotation.getAccession() != null && !this.pantherData.getAccessions().contains(annotation.getAccession()))
			this.pantherData.getAccessions().add(annotation.getAccession());
		//"public_id" - not indexed
		//"sf_id"
		if (annotation.getSf_id() != null && !this.pantherData.getSf_ids().contains(annotation.getSf_id()))
			this.pantherData.getSf_ids().add(annotation.getSf_id());
		//"sf_name"
		if (annotation.getSf_name() != null && !this.pantherData.getSf_names().contains(annotation.getSf_name()))
			this.pantherData.getSf_names().add(annotation.getSf_name());
		//"prop_sf_id" - not indexed
		//"species" - only add the top species from the list
		if (annotation.getSpecies() != null
				&& !this.pantherData.getSpecies_list().contains(annotation.getSpecies())
				&& this.pantherData.getSpecies_list().size() == 0)
			this.pantherData.getSpecies_list().add(annotation.getSpecies());
		//"taxonomic_range"
		if (annotation.getTaxonomic_range() != null && !this.pantherData.getTaxonomic_ranges().contains(annotation.getTaxonomic_range()))
			this.pantherData.getTaxonomic_ranges().add(annotation.getTaxonomic_range());
		//"tree_node_type" - not indexed
		//"event_type" = not indexed
		//"branch_length"
		if (annotation.getBranch_length() != null && !this.pantherData.getBranch_lengths().contains(annotation.getBranch_length()))
			this.pantherData.getBranch_lengths().add(annotation.getBranch_length());
		//"gene_id"
		if (annotation.getGene_id() != null && !this.pantherData.getGene_ids().contains(annotation.getGene_id()))
			this.pantherData.getGene_ids().add(annotation.getGene_id());
		//"gene_symbol"
		if (annotation.getGene_symbol() != null && !this.pantherData.getGene_symbols().contains(annotation.getGene_symbol()))
			this.pantherData.getGene_symbols().add(annotation.getGene_symbol());
		//"definition"
		if (annotation.getDefinition() != null && !this.pantherData.getDefinitions().contains(annotation.getDefinition()))
			this.pantherData.getDefinitions().add(annotation.getDefinition());
		//"node_name" -> "uniprotId"
		if (annotation.getNode_name() != null && !this.pantherData.getNode_names().contains(annotation.getNode_name())) {
			this.pantherData.getNode_names().add(annotation.getNode_name());
			// DICDI|dictyBase=DDB_G0277745|UniProtKB=Q86KT5
			int pos = annotation.getNode_name().indexOf("UniProtKB");
			String uniProtId = null;
			if(pos != -1) {
				uniProtId = annotation.getNode_name().substring(pos + 10);
				if(!this.pantherData.getUniprot_ids().contains(uniProtId))
					this.pantherData.getUniprot_ids().add(uniProtId);
			}
		}
		//"organism"
		if (annotation.getOrganism() != null && !this.pantherData.getOrganisms().contains(annotation.getOrganism()))
			this.pantherData.getOrganisms().add(annotation.getOrganism());
	}

	public PantherData readBookById(String id) throws Exception {

		this.pantherData = new PantherData();
		this.annotations = new ArrayList<Annotation>();

		System.out.println("Reading book: " + id);
		readPantherXmlToObject(id);
		try {
			flattenTree(pantherData.getSearch().getAnnotation_node().getChildren());
		}
		catch(Exception e) {
			System.out.println("Error "+ e);
		}
		buildListItems();
		
//		this.pantherData.setFamily_name(getFamilyName(id));

		return this.pantherData;
	}

	public PantherData convertJsonToSolrforApi(String jsonString, String id) throws Exception {
		this.annotations = new ArrayList<Annotation>();
		// convert json string to Panther object
		this.pantherData = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT).readValue(jsonString,
				PantherData.class);
		this.pantherData.setId(id);
		this.pantherData.setJsonString(jsonString);
		this.pantherData.setFamily_name("familyName");
		try {
			if(this.pantherData.getSearch() == null) {
				System.out.println("Error in: " + this.pantherData.getId());
			} else {
				Annotation rootNodeAnnotation = this.pantherData.getSearch().getAnnotation_node();
				if(rootNodeAnnotation == null) {
//					System.out.println(this.pantherData.getSearch().getParameters().getBook());
					this.pantherData = null;
				} else {
					addToListFromAnnotation(rootNodeAnnotation);
					flattenTree(this.pantherData.getSearch().getAnnotation_node().getChildren());
					buildListItems();
				}
			}
		} catch(Exception e) {
			e.printStackTrace();
			System.out.println("Error in building list: " + e.getMessage());
		}

		return this.pantherData;
	}

	public PantherData convertJsonToSolrDocument(PantherData orig, String familyName) throws Exception {
		this.annotations = new ArrayList<Annotation>();
		String jsonString = orig.getJsonString();

		// convert json string to Panther object
		this.pantherData = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT).readValue(jsonString,
				PantherData.class);
		this.pantherData.setId(orig.getId());
		this.pantherData.setJsonString(jsonString);
		this.pantherData.setFamily_name(familyName);
		try {
			if(this.pantherData.getSearch() == null) {
				System.out.println("Error in: " + this.pantherData.getId());
			} else {
				Annotation rootNodeAnnotation = this.pantherData.getSearch().getAnnotation_node();
				if(rootNodeAnnotation == null) {
					this.pantherData = null;
				} else {
					addToListFromAnnotation(rootNodeAnnotation);
					flattenTree(this.pantherData.getSearch().getAnnotation_node().getChildren());
					buildListItems();
				}
			}
		} catch(Exception e) {
			e.printStackTrace();
			System.out.println("Error in building list: " + e.getMessage());
		}

		return this.pantherData;
	}

	public List<String> getFieldValue(PantherData orig) throws Exception {
		String jsonString = orig.getJsonString();
		List<String> fieldValue = new ArrayList<>();
		// convert json string to Panther object
		this.pantherData = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT).readValue(jsonString,
										PantherData.class);
		try {
			Annotation rootNodeAnnotation = this.pantherData.getSearch().getAnnotation_node();
			fieldValue.add(rootNodeAnnotation.getSpecies());
		} catch(NullPointerException e) {
			return null;
		}
		return fieldValue;
	}

	public boolean isHoriz_Transfer(PantherData orig) throws Exception {
		this.annotations = new ArrayList<Annotation>();
		String jsonString = orig.getJsonString();

			// convert json string to Panther object
		this.pantherData = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT).readValue(jsonString,
																		PantherData.class);
		this.pantherData.setId(orig.getId());
		if(this.pantherData.getSearch() != null) {
			Annotation rootNodeAnnotation = this.pantherData.getSearch().getAnnotation_node();
			this.annotations.add(rootNodeAnnotation);
			if(rootNodeAnnotation != null) {
				flattenTree(this.pantherData.getSearch().getAnnotation_node().getChildren());
			}
		}
		List<String> pantherWithHorizTrans = new ArrayList<>();
		for(int i = 0; i < this.annotations.size(); i++) {
			Annotation currAnno = this.annotations.get(i); //&&
			if(currAnno!= null && currAnno.getEvent_type() != null) {
				if(currAnno.getEvent_type().equals("HORIZ_TRANSFER")) {
					return true;
				}
			}
		}
		return false;
	}

	public boolean hasPlantGenome(PantherData orig) throws Exception {
		this.annotations = new ArrayList<Annotation>();
		String jsonString = orig.getJsonString();

		// convert json string to Panther object
		this.pantherData = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT).readValue(jsonString,
				PantherData.class);
		this.pantherData.setId(orig.getId());
		if(this.pantherData.getSearch() != null) {
			Annotation rootNodeAnnotation = this.pantherData.getSearch().getAnnotation_node();
			if(rootNodeAnnotation == null) {
				return true;
			}
			this.annotations.add(rootNodeAnnotation);
			if(rootNodeAnnotation != null) {
				flattenTree(this.pantherData.getSearch().getAnnotation_node().getChildren());
			}
		}

		for(int i = 0; i < this.annotations.size(); i++) {
			Annotation currAnno = this.annotations.get(i); //&&
			if(currAnno!= null && currAnno.getOrganism() != null) {
				if(getPlantOrganisms().contains(currAnno.getOrganism())) {
					return true;
				}
			}
		}

		return false;
	}

	private List<String> getPlantOrganisms() {
		List<String> genomes = Arrays.asList(
				"Amborella trichopoda",
				"Arabidopsis thaliana",
				"Brachypodium distachyon",
				"Brassica napus",
				"Brassica rapa subsp. Pekinensis",
				"Capsicum annuum",
				"Chlamydomonas reinhardtii",
				"Citrus sinensis",
				"Cucumis sativus",
				"Erythranthe guttata",
				"Eucalyptus grandis",
				"Glycine max",
				"Gossypium hirsutum",
				"Helianthus annuus",
				"Hordeum vulgare subsp. vulgare",
				"Juglans regia",
				"Lactuca sativa",
				"Manihot esculenta",
				"Medicago truncatula",
				"Musa acuminata subsp. malaccensis",
				"Nelumbo nucifera",
				"Nicotiana tabacum",
				"Oryza sativa",
				"Ostreococcus tauri",
				"Phoenix dactylifera",
				"Physcomitrella patens",
				"Populus trichocarpa",
				"Prunus persica",
				"Ricinus communis",
				"Selaginella moellendorffii",
				"Setaria italica",
				"Solanum lycopersicum",
				"Solanum tuberosum",
				"Sorghum bicolor",
				"Spinacia oleracea",
				"Theobroma cacao",
				"Triticum aestivum",
				"Vitis vinifera",
				"Zea mays",
				"Zostera marina");
		return genomes;
	}

	public PantherData readBookFromLocal(PantherData oldPantherData) throws Exception {
		this.pantherData = new PantherData();
		this.annotations = new ArrayList<Annotation>();
		this.pantherData.setId(oldPantherData.getId());
		this.pantherData.setJsonString(oldPantherData.getJsonString());

		String jsonString = oldPantherData.getJsonString();
		// convert json string to Panther object
		PantherData oldPantherData2 = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT).readValue(jsonString,
				PantherData.class);

		try {
			if(oldPantherData2.getSearch() == null) {
				System.out.println("Error in: " + oldPantherData.getId());
			} else {
				Annotation rootNodeAnnotation = oldPantherData2.getSearch().getAnnotation_node();
				addToListFromAnnotation(rootNodeAnnotation);
				flattenTree(oldPantherData2.getSearch().getAnnotation_node().getChildren());
				buildListItems();
			}

			this.pantherData.setFamily_name(oldPantherData.getFamily_name());
		} catch(Exception e) {
			e.printStackTrace();
			System.out.println("Error in building list: " + e.getMessage());
		}

		return this.pantherData;
	}

}
