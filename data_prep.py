import os
import json
import re
from datamule import Portfolio
from tqdm import tqdm

portfolio = Portfolio("8k_2024")

# Configuration
batch_size = 1000  # Number of submissions per batch
output_dir = "8k_2024_parsed_text"
output_file_prefix = "portfolio_data"
batch_data = []
batch_count = 0
file_count = 0
sub_count = 0
total_text_records = 0
total_table_records = 0

# Create output directory if it doesn't exist
os.makedirs(output_dir, exist_ok=True)

# Pre-compile regex for text validation
TEXT_PATTERN = re.compile(r'[a-zA-Z]')

# Open output file for streaming
current_output_file = None
current_batch_num = 0

def get_output_file():
    """Get or create output file handle for current batch"""
    global current_output_file, current_batch_num
    
    if current_output_file is None:
        filename = f"{output_file_prefix}_batch_{current_batch_num:04d}.jsonl"
        filepath = os.path.join(output_dir, filename)
        current_output_file = open(filepath, 'w', encoding='utf-8')
    
    return current_output_file

def close_current_file():
    """Close current output file"""
    global current_output_file
    if current_output_file:
        current_output_file.close()
        current_output_file = None

def write_record(record):
    """Write a single record to current output file"""
    f = get_output_file()
    json.dump(record, f, ensure_ascii=False)
    f.write('\n')

def extract_all_content_from_json(data, path_components=None, accession=None, path=None):
    """Extract both text and table data in a single pass through JSON"""
    global total_text_records, total_table_records
    
    if path_components is None:
        path_components = []
    
    if isinstance(data, dict):
        for key, value in data.items():
            new_path_components = path_components + [str(key)]
            
            # Handle text fields
            if key == "text" and isinstance(value, str) and TEXT_PATTERN.search(value):
                json_path = '.'.join(path_components) if path_components else ""
                record = {
                    "accession": accession,
                    "path": path,
                    "json_path": json_path,
                    "type": "text",
                    "text": value.strip()
                }
                write_record(record)
                total_text_records += 1
            
            # Handle table fields  
            elif key == "table" and isinstance(value, list) and len(value) > 1:
                json_path = '.'.join(path_components) if path_components else ""
                
                # Assume first row is headers
                headers = value[0]
                data_rows = value[1:]
                
                # Create a record for each column
                for col_idx, header in enumerate(headers):
                    # Build column values list efficiently
                    column_values = [
                        row[col_idx] if col_idx < len(row) else ""
                        for row in data_rows
                    ]
                    
                    # Build column text more efficiently
                    # TODO, should modify to exclude if only column names has letters and rows numeric or otherwise.
                    if column_values:  # Only process if we have values
                        column_text = f"{header}: {', '.join(str(v) for v in column_values if v)}"
                        
                        if column_text and len(column_text) > len(header) + 2:  # More than just "header: "
                            record = {
                                "accession": accession,
                                "path": path,
                                "json_path": json_path,
                                "type": "table_column",
                                "text": column_text
                            }
                            write_record(record)
                            total_table_records += 1
            
            # Continue recursion
            else:
                extract_all_content_from_json(value, new_path_components, accession, path)
    
    elif isinstance(data, list):
        for i, item in enumerate(data):
            new_path_components = path_components + [f"[{i}]"]
            extract_all_content_from_json(item, new_path_components, accession, path)

def start_new_batch():
    """Start a new batch file"""
    global current_batch_num, batch_data
    
    close_current_file()
    print(f"Completed batch {current_batch_num} with {len(batch_data)} records")
    current_batch_num += 1
    batch_data = []

# Process portfolio with tqdm progress bar for submissions
for sub in tqdm(portfolio, desc="Processing submissions"):
    accession = sub.metadata.content['accession-number']

    # Validation check if any item is a list - issue w/metadata in malformed sgml
    if isinstance(accession, list):
        accession = accession[0]

    for doc in sub:
        if doc.extension not in ['.htm','.html','.txt']:
            continue
        elif doc.type == 'XML':
            continue
        
        doc_path = doc.path
        
        # Parse the JSON data and extract all content in single pass
        try:
            doc_data = doc.data  # This should be the JSON structure
            
            # Extract both text and tables in a single traversal
            extract_all_content_from_json(doc_data, accession=accession, path=doc_path)
                    
        except Exception as e:
            print(f"Error processing document {doc_path} in submission {accession}: {e}")
            continue

        file_count += 1

    sub_count += 1
    batch_data.append(f"sub_{sub_count}")  # Just track batch progress

    # Start new batch when reaching batch_size submissions
    if sub_count % batch_size == 0:
        start_new_batch()

# Close final file
close_current_file()

print(f"Processing complete. Total files processed: {file_count}")
print(f"Total submissions processed: {sub_count}")
print(f"Total text records extracted: {total_text_records}")
print(f"Total table column records extracted: {total_table_records}")
print(f"Total records: {total_text_records + total_table_records}")
print(f"Total batches created: {current_batch_num + 1}")
print(f"Output directory: {output_dir}")