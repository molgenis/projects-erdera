requirements to run script: 
1. the new value fields in ontology mappings need to correspond to the RD3 ontologies 

## Scripts 
### mapping_cnag_experiments_to_rd3
This script maps the CNAG (GPAP) data from the staging area to RD3. 

- Agents
- Endpoints
- Resources
- NGS Sequencing (based on experiments)

Uses `Experiment types`, `kits`, `Library source`, `Tissue types`, and `Erns` from `Ontology mappings` schema. The prerequisites are that the values in the `new value` column correspond to the RD3 ontology. The script does not import into the ontologies. 

### mapping_cnag_to_rd3
This script maps the CNAG (GPAP) participant data to RD3. 

- Pedigree
- Individuals 
- Pedigree members (disabled at the moment)
- Clinical observations
- Consent
- Disease history 
- Phenotype observations

## mention order in which to run the scripts 
## mention to do's and incompletes 
## explain the molgenis schema's and for what they are used. 