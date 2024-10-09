'''A script for annotating the amino acid sequences using InterProScan and Kofamscan.'''
from tqdm import tqdm 
import subprocess
import os 
import argparse 

# Paths to the InterProScan and Kofamscan programs. 
INTERPROSCAN = '/home/prichter/interproscan/interproscan-5.69-101.0/interproscan.sh'
KOFAMSCAN = '/home/prichter/kofamscan/kofam_scan-1.3.0/exec_annotation'

# Structure of the GTDB version directories is as follows:
# /var/lib/pgsql/data/gtdb/r{version}/
#   ./annotations/pfam
#   ./annotations/kegg
#   ./proteins/amino_acids/
#   ./proteins/nucleotides/
#   ./metadata/


def annotate_pfam(input_path:str, output_dir:str):
    '''Runs InterProScan on the FASTA file specified by the input path. This file should contain amino acid sequences.'''
    subprocess.run(f"sed -i 's/\*//g' {input_path}", shell=True, check=True) # Remove all asterisks from the file, as these will throw an error.
    # Documentation is here: https://interproscan-docs.readthedocs.io/en/latest/HowToRun.html 
    subprocess.run(f'sh {INTERPROSCAN} -i {input_path} -d {output_dir} -appl Pfam', shell=True, check=True)


def annotate_kegg(input_path:str, output_dir:str, config:str='/home/prihter/kofamscan/config.yml'):

    output_path = input_path + '.ko'
    # Instructions here: https://taylorreiter.github.io/2019-05-11-kofamscan/
    subprocess.run(f'.{KOFAMSCAN} {input_path} -o {output_path} --config {config} --format detail-tsv', shell=True, check=True)



if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--type', choices=['pfam', 'kegg'], type=str, default='kegg')
    parser.add_argument('--data-dir', type=str, default='/var/lib/pgsql/data/gtdb/')
    parser.add_argument('--version', default=220, type=int)

    args = parser.parse_args()

    proteins_dir = os.path.join(args.data_dir, f'r{int(args.version)}', 'proteins', 'amino_acids')

    # Make a directory to store the annotations...
    annotations_dir = os.path.join(args.data_dir, f'r{int(args.version)}', 'annotations', args.type)
    os.makedirs(annotations_dir, exist_ok=True)

    if args.type == 'pfam':
        for file in tqdm(os.listdir(proteins_dir), desc='Annotating amino acid sequences with Pfam.'):
            annotate_pfam(input_path=os.path.join(proteins_dir, file), output_dir=annotations_dir)
    
    elif args.type == 'kegg':
        for file in tqdm(os.listdir(proteins_dir), desc='Annotating amino acid sequences with KEGG ortho groups.'):
            annotate_kegg(input_path=os.path.join(proteins_dir, file), output_dir=annotations_dir)