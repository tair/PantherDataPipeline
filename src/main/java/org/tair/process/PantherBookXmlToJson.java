package org.tair.process;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.json.JSONObject;
import org.tair.module.Annotation;
import org.tair.module.Children;
import org.tair.module.PantherData;
import org.tair.util.Util;

import java.util.ArrayList;
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

	private void addToListFromAnnotation(Annotation annotation) {

			if (annotation.getOrganism() != null && !this.pantherData.getOrganisms().contains(annotation.getOrganism()))
				this.pantherData.getOrganisms().add(annotation.getOrganism());

			if (annotation.getGene_symbol() != null
					&& !this.pantherData.getGene_symbols().contains(annotation.getGene_symbol()))
				this.pantherData.getGene_symbols().add(annotation.getGene_symbol());

			if (annotation.getNode_name() != null
					&& !this.pantherData.getNode_names().contains(annotation.getNode_name())) {
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

			if (annotation.getDefinition() != null
					&& !this.pantherData.getDefinitions().contains(annotation.getDefinition()))
				this.pantherData.getDefinitions().add(annotation.getDefinition());

			if (annotation.getBranch_length() != null
					&& !this.pantherData.getBranch_lengths().contains(annotation.getBranch_length()))
				this.pantherData.getBranch_lengths().add(annotation.getBranch_length());

			if (annotation.getAccession() != null
					&& !this.pantherData.getAccessions().contains(annotation.getAccession()))
				this.pantherData.getAccessions().add(annotation.getAccession());

			if (annotation.getGene_id() != null && !this.pantherData.getGene_ids().contains(annotation.getGene_id()))
				this.pantherData.getGene_ids().add(annotation.getGene_id());

			if (annotation.getSf_name() != null && !this.pantherData.getSf_names().contains(annotation.getSf_name()))
				this.pantherData.getSf_names().add(annotation.getSf_name());

			if (annotation.getSf_id() != null && !this.pantherData.getSf_ids().contains(annotation.getSf_id()))
				this.pantherData.getSf_ids().add(annotation.getSf_id());

			if (annotation.getNode_type() != null
					&& !this.pantherData.getEvent_types().contains(annotation.getNode_type()))
				this.pantherData.getEvent_types().add(annotation.getNode_type());

			if (annotation.getSpeciation_event() != null && !this.pantherData.getSpeciation_events()
					.contains(annotation.getReference_speciation_event()))
				this.pantherData.getSpeciation_events().add(annotation.getSpeciation_event());

			if (annotation.getSpecies() != null
					&& !this.pantherData.getSpecies_list().contains(annotation.getSpecies())
					&& this.pantherData.getSpecies_list().size() == 0)
				this.pantherData.getSpecies_list().add(annotation.getSpecies());
	}
	
	private String getFamilyName(String id) throws Exception {
		
		String url = "http://pantherdb.org/tempFamilySearch?type=family_name&book=" + id;
		String jsonString = Util.readContentFromWebUrlToJson(PantherData.class, url);
		return new JSONObject(jsonString).getJSONObject("search").getString("family_name");

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
		
		this.pantherData.setFamily_name(getFamilyName(id));

		return this.pantherData;

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
			Annotation rootNodeAnnotation = oldPantherData2.getSearch().getAnnotation_node();
			addToListFromAnnotation(rootNodeAnnotation);
			flattenTree(oldPantherData2.getSearch().getAnnotation_node().getChildren());
			buildListItems();

			this.pantherData.setFamily_name(getFamilyName(oldPantherData.getId()));
		} catch(Exception e) {
			System.out.println(e);
		}

		return this.pantherData;
	}

}
