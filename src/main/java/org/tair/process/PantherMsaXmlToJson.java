package org.tair.process;

import java.util.ArrayList;
import java.util.List;

import org.tair.module.Annotation;
import org.tair.module.Children;
import org.tair.module.PantherData;
import org.tair.util.Util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

public class PantherMsaXmlToJson {

	PantherData pantherData = null;
	private List<Annotation> annotations = null;

	public PantherData readPantherXmlToObject(String id) throws Exception {

		// read the xml data from web.
		 String url = "http://pantherdb.org/tempFamilySearch?type=msa_info&book=" + id;
		 String jsonString = Util.readContentFromWebUrlToJson(PantherData.class, url);

		// convert json string to Panther object
		this.pantherData = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT).readValue(jsonString,
				PantherData.class);
		this.pantherData.setMsaJsonString(jsonString);
		return this.pantherData;

	}

	public void flattenTree(Children children) {

		if (children == null || children.getAnnotation_node() == null || children.getAnnotation_node().size() == 0)
			return;

		for (Annotation annotation : children.getAnnotation_node()) {
			this.annotations.add(annotation);
			flattenTree(annotation.getChildren());
		}
	}

	public void buildListItems() {

		for (Annotation annotation : this.annotations) {
			// System.out.println(annotation.getAccession());
			if (annotation.getOrganism() != null && !this.pantherData.getOrganisms().contains(annotation.getOrganism()))
				this.pantherData.getOrganisms().add(annotation.getOrganism());

			if (annotation.getGene_symbol() != null
					&& !this.pantherData.getGene_symbols().contains(annotation.getGene_symbol()))
				this.pantherData.getGene_symbols().add(annotation.getGene_symbol());

			if (annotation.getNode_name() != null
					&& !this.pantherData.getNode_names().contains(annotation.getNode_name()))
				this.pantherData.getNode_names().add(annotation.getNode_name());

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
					&& !this.pantherData.getNode_types().contains(annotation.getNode_type()))
				this.pantherData.getNode_types().add(annotation.getNode_type());

			if (annotation.getReference_speciation_event() != null && !this.pantherData.getReference_speciation_events()
					.contains(annotation.getReference_speciation_event()))
				this.pantherData.getReference_speciation_events().add(annotation.getReference_speciation_event());

			if (annotation.getSpecies() != null
					&& !this.pantherData.getSpecies_list().contains(annotation.getSpecies()))
				this.pantherData.getSpecies_list().add(annotation.getSpecies());

		}
	}

	public PantherData readMsaById(String id) throws Exception {

		this.pantherData = new PantherData();
		this.annotations = new ArrayList<Annotation>();

		System.out.println("Reading MSA: " + id);
		readPantherXmlToObject(id);
		// flattenTree(pantherData.getSearch().getAnnotation_node().getChildren());
		// buildListItems();

		return this.pantherData;

	}

	public static void main(String s[]) throws Exception {

		PantherMsaXmlToJson msa = new PantherMsaXmlToJson();
		msa.readMsaById("PTHR10000");

	}

}
