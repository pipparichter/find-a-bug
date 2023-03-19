<p align="center">
<img src="./mascot.png" width="400" height="400">
</p>

# Find-A-Bug

## About the database

### Where is the data hosted?

The data is hosted on a Caltech machine running a Red Hat Linux distro, `microbes.gps.caltech.edu`. This remote host is only accessible when on Caltech campus wifi or using a VPN.

### How is the data stored?

The data is stored in multiple tables in a MariaDB SQL database. More information on the table structure and the information they contain is given in the following section. 

### What information does the database contain?

Currently, the data is organized into three tables (although we will be adding more soon!). All data is from the Genome Taxonomy Database (GTDB) r207 release. 

1. `gtdb_r207_metadata`
2. `gtdb_r207_amino_acid_seqs`
3. `gtdb_r207_annotations_kegg`

