import csv
import io
import sys
import zipfile

EXPECTED_DIRS = [
    'IFC Output/IFC Models/',
    'IFC Output/IFC Classification/',
    'IFC Output/IFC Object Type/',
    'IFC Output/IFC Project/',
    'IFC Output/IFC Properties/',
    'IFC Output/IFC Pset Template/',
    'IFC Output/IFC Spatial Structure/',
    'IFC Output/IFC System/',
]

EXPECTED_HEADERS = {
    'IFC Output/IFC Models/IFC MODEL TABLE.csv': ['Source_Path','Source_File','File_Codes','Model_Schema_Status','Date_Checked'],
}

def main(path):
    with zipfile.ZipFile(path) as z:
        names = set(z.namelist())
        for d in EXPECTED_DIRS:
            if not any(n.startswith(d) for n in names):
                raise SystemExit(f'Missing folder: {d}')
        for f, hdr in EXPECTED_HEADERS.items():
            if f not in names:
                raise SystemExit(f'Missing file: {f}')
            data = z.read(f).decode('utf-8', errors='ignore')
            first = next(csv.reader(io.StringIO(data)), [])
            if first != hdr:
                raise SystemExit(f'Header mismatch for {f}: {first}')
    print('OK')

if __name__ == '__main__':
    if len(sys.argv) != 2:
        raise SystemExit('usage: validate_ifc_qa_zip.py <zip>')
    main(sys.argv[1])
