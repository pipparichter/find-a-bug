'''A script for annotating the amino acid sequences using InterProScan and Kofamscan.'''
from tqdm import tqdm 
import subprocess
import os 
import argparse 
import gzip
import zipfile 
import tarfile 
import shutil 
from typing import NoReturn

# Paths to the InterProScan and Kofamscan programs. 
INTERPROSCAN = '/home/prichter/interproscan/interproscan-5.70-102.0/interproscan.sh'
KOFAMSCAN = '/home/prichter/kofamscan/kofam_scan-1.3.0/exec_annotation'

# It seems like compressing multiple times will do absolutely nothing for saving space, and might just make things worse... 
# https://stackoverflow.com/questions/1166385/how-many-times-can-a-file-be-compressed

# https://superuser.com/questions/173756/which-is-more-efficient-tar-or-zip-compression-what-is-the-difference-between


def compress(path:str, archive:tarfile.TarFile):
    '''Add a file at path to an open tar archive, which should be using gz compression. If
    specified, remove the original file.'''

    # Specifying the arcname as the filename should just dump the file into the root directory 
    # of the archive. 



def extract(archive:tarfile.TarFile, name:str, output_dir:str='.') -> str:
    '''Read the file specified by member from the compressed archive specified at path, and write the
    output to a file at path. 
    
    :param archive: An open tar archive. 
    :param name: The name of the archive member. Note that the name should be a relative path to the 
        member, i.e. archaea/RS_GCA_*.faa.gz.
    :param output_dir: The path where the extracted file will be written. 
    :returns: The path to the output file. 
    '''

    member = archive.getmember(name)
    content = f.extractfile(member).read()

    output_file_path = os.path.join(output_dir, os.path.basename(name))
    with open(output_file_path, 'wb') as f:
        f.write(content)

    return output_file_path
        

def annotate_pfam(input_path:str, annotations_archive:str) -> str:
    '''Runs InterProScan on the FASTA file specified by the input path. This file should contain amino acid sequences.'''
    subprocess.run(f"sed -i 's/\*//g' {input_path}", shell=True, check=True) # Remove all asterisks from the file, as these will throw an error.
    # Documentation is here: https://interproscan-docs.readthedocs.io/en/latest/HowToRun.html 
    # The -dra option should speed things up by disabling residue-level calculations. 
    # I disabled precalc because it didn't seem to be working (though speeds things up in theory).
    subprocess.run(f'sh {INTERPROSCAN} -i {input_path} -d {output_dir} -appl Pfam -dra --format TSV --disable-precalc', shell=True, check=True)

    output_file_name = os.path.basename(input_path) + '.tsv' # Default output path just appends a TSV. 
    output_path = os.path.join(output_dir, output_file_name)
    return output_path


def annotate_kegg(input_path:str, output_dir:str, config:str='/home/prichter/kofamscan/config.yml') -> str:

    return ''
    # output_path = input_path + '.ko'
    # # Instructions here: https://taylorreiter.github.io/2019-05-11-kofamscan/
    # subprocess.run(f'.{KOFAMSCAN} {input_path} -o {output_path} --config {config} --format detail-tsv', shell=True, check=True)

    # return output_path


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--type', choices=['pfam', 'kegg'], type=str, default='kegg')
    parser.add_argument('--data-dir', type=str, default='/var/lib/pgsql/data/gtdb/')
    parser.add_argument('--version', default=220, type=int)

    args = parser.parse_args()

    # Open the archive of proteins in read mode. 
    proteins_archive_path = os.path.join(args.data_dir, f'r{version}', 'proteins_aa.tar.gz') 
    proteins_archive = tarfile.open(proteins_archive_path, 'r:gz')

    # Create and open (write mode) a new tar archive to store the annotation data. 
    annotations_archive_path = os.path.join(args.data_dir, f'r{version}', f'annotations_{args.type}.tar')
    annotations_archive = tarfile.open(annotations_archive_path, 'a') # Make sure to open this in append mode. 
    
    func = annotate_pfam if (args.type == 'pfam') else annotate_kegg
    names = proteins_archive.getnames()

    for name in tqdm(names, desc='Annotating amino acid sequences...'):
        # Only proceed with the annotation if the file does not already exist. 
        proteins_file_path = extract(proteins_archive, name=name, output_dir=args.data_dir) # This is the default output filename. 

        # Only annotate if it is not already present in the directory. 
        if not (os.path.basename(annotations_file_path) + '.tsv' in annotations_archive.getnames()):
            annotations_file_path = func(proteins_file_path)
            annotations_archive.add(annotations_file_path, arcname=os.path.basename(annotations_file_path))

        # Clean up the temporary files; should be added to the archive. 
        os.remove(proteins_file_path)
        os.remove(annotations_file_path)
    
    # Close the archives... 
    annotations_archive.close()
    proteins_archive.close()

    # Don't want to delete the un-compressed file yet, in case something fails. 
    with tarfile.open(annotations_archive_path, 'r') as f_in:
        with gzip.open(annotations_archive_path + '.gz', 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)