## Biocache Spark 
A collection on Spark services for working with Biocache based content (Cassandra, SOLR).
  
This is currently an exploratory project.

### Export to GBIF

For each resource the following sequence is run:
  1. Export the processed content from within Cassandra (?/ SOLR) into a CSV
  2. Package as a DwC-A with the EML from the Collectory
  3. Deterine if the dataset is eligible for registration with GBIF
    - if eligible and not registered, then register in GBIF and store the GBIF dataset UUID in the Collectory
    - if eligible and registered, then trigger a crawl by GBIF if appropriate

