'''A script for annotating the amino acid sequences using InterProScan and Kofamscan.'''

import subprocess
import os 
import argparse 

INTERPROSCAN = '/home/prichter/interproscan/interproscan-5.69-101.0/interproscan.sh'
KOFAMSCAN = ''

def annotate_pfam(input_path:str, output_path:str):
    '''Runs InterProScan on the FASTA file specified by the input path. This file should contain amino acid sequences.'''
    
    subprocess.run(f'{INTERPROSCAN} -i {input_path} -o {output_path}')

def annotate_kegg():
    pass 


if __name__ == '__main__':
    parser = parser.ArgumentParser()
    parser.add_argument('--type', choices=['pfam', 'kegg'], type=str, default=kegg)
    parser.add_argument('--data-dir', type=str, default='/var/lib/pgsql/data/gtdb/')
    parser.add_argument('--version', default=220, type=int)

    args = parser.parse_args()

    proteins_dir = os.path.join(args.data_dir, f'r{int(version)}', 'proteins')

    # Make a directory to store the annotations...
    annotations_dir = os.path.join(args.data_dir, f'r{int(version)}', 'annotations', args.type)
    os.makedirs(annotations_dir)