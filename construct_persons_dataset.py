import json
import os
import re
from pathlib import Path
from nameparser import HumanName

def process_name(name):
    name = name.strip()
    if not re.match(r'^[a-zA-Z\s\-\.]+$', name):
        return None
    parsed = HumanName(name)
    if not parsed.first or not parsed.last:
        return None
    if len(parsed.first) < 3 or len(parsed.last) < 3:
        return None
    
    return f"{parsed.first.title()} {parsed.last.title()}"

def extract_filename_from_path(path):
    """Extract the last part of the path (e.g., something.htm)"""
    if '::' in path:
        # Handle the case where path contains "::" separator
        path = path.split('::')[-1]
    return os.path.basename(path)

def process_records_directory(records_dir='records'):
    """Process all JSON files in the records directory"""
    records_path = Path(records_dir)
    
    if not records_path.exists():
        print(f"Directory {records_dir} does not exist!")
        return
    
    all_names = set()
    results = []
    
    # Process each JSONL file
    for jsonl_file in records_path.glob('*.jsonl'):
        print(f"Processing {jsonl_file.name}...")
        
        try:
            with open(jsonl_file, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue
                    
                    try:
                        data = json.loads(line)
                        
                        # Extract accession and path
                        accession = data.get('accession', '')
                        path = data.get('path', '')
                        filename = extract_filename_from_path(path)
                        
                        # Get persons from entities
                        entities = data.get('entities', {})
                        persons = entities.get('PERSON', [])
                        
                        # Add all person names to the set for processing
                        for person in persons:
                            all_names.add(person)
                        
                        # Store the record info
                        results.append({
                            'accession': accession,
                            'filename': filename,
                            'original_persons': persons
                        })
                        
                    except json.JSONDecodeError as e:
                        print(f"Error parsing line {line_num} in {jsonl_file.name}: {e}")
            
        except Exception as e:
            print(f"Error processing {jsonl_file.name}: {e}")
    
    print(f"\nFound {len(all_names)} unique person names across all files")
    
    # Process all names through the name processor
    processed_names = set()
    name_mapping = {}
    
    for name in all_names:
        processed = process_name(name)
        if processed:
            processed_names.add(processed)
            name_mapping[name] = processed
     
    
    print(f"After processing: {len(processed_names)} valid names")
    
    # Apply processed names to each record
    final_results = []
    for record in results:
        processed_persons = set()
        for original_name in record['original_persons']:
            if original_name in name_mapping:
                processed_persons.add(name_mapping[original_name])
        
        final_results.append({
            'accession': record['accession'],
            'filename': record['filename'],
            'persons': list(processed_persons)
        })
    
    # Save results to JSON file
    output_file = 'persons/persons.json'  # Change this path if needed
    # Alternative: output_file = os.path.join(records_dir, 'processed_persons.json')  # Save in records directory
    # Alternative: output_file = '/path/to/your/desired/output/processed_persons.json'  # Absolute path
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(final_results, f, indent=2, ensure_ascii=False)
    
    print(f"\nResults saved to {output_file}")
    print(f"Processed {len(final_results)} records")
    
    # Print summary statistics
    total_persons = sum(len(record['persons']) for record in final_results)
    print(f"Total person entries across all records: {total_persons}")
    print(f"Unique processed names: {len(processed_names)}")
    
    # Show sample of processed names
    print(f"\nSample of processed names:")
    for i, name in enumerate(sorted(processed_names)):
        if i >= 10:  # Show first 10
            break
        print(f"  - {name}")
    
    return final_results

if __name__ == "__main__":
    # Run the processing
    RECORDS_DIR = 'records'
    results = process_records_directory(RECORDS_DIR)