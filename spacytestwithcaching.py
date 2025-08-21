import spacy
import json
import pickle
import hashlib
import os
import glob
from time import time
from tqdm import tqdm

def get_cache_key(text):
    return hashlib.md5(text.encode()).hexdigest()

def extract_entities_from_doc(doc):
    """Extract entities from spaCy doc and return as dictionary"""
    entities = {}
    for ent in doc.ents:
        if ent.label_ not in entities:
            entities[ent.label_] = set()
        entities[ent.label_].add(ent.text)
    return {k: list(v) for k, v in entities.items()}

def extract_all_entities_from_jsonl(input_file, output_file, cache_file="entity_cache.pkl"):
    # Load existing cache to test what needs processing
    cache = {}
    if os.path.exists(cache_file):
        with open(cache_file, 'rb') as f:
            cache = pickle.load(f)
        print(f"Loaded cache with {len(cache)} entries")
    
    # Read JSONL and split into text segments
    all_text = []
    text_metadata = []
    total_chars = 0
    
    with open(input_file, 'r', encoding='utf-8') as f:
        for line in f:
            data = json.loads(line.strip())
            # Split text into segments (previously called paragraphs)
            text_segments = [p.strip() for p in data['text'].split('\n\n') if p.strip()]
            
            # Store each text segment with its document metadata
            for text_segment in text_segments:
                all_text.append(text_segment)
                total_chars += len(text_segment)
                text_metadata.append({
                    'accession': data['accession'], 
                    'path': data['path']
                })
    
    # Calculate and print token estimate
    estimated_tokens = total_chars / 4
    print(f"Total characters: {total_chars:,}")
    print(f"Estimated tokens (chars/4): {estimated_tokens:,.0f}")
    print(f"Processing {len(all_text)} text segments from approximately {len(set((meta['accession'], meta['path']) for meta in text_metadata))} documents")
    
    # Test cache and identify what needs processing
    texts_to_process = []
    text_indices = []  # Track which original texts need processing
    cached_entities = []  # Store entities for each text (from cache or processing)
    cache_hits = 0
    new_cache_entries = {}  # Store new entries to add to cache
    
    for i, text in enumerate(all_text):
        cache_key = get_cache_key(text)
        
        if cache_key in cache:
            # Use cached result
            cached_entities.append(cache[cache_key])
            cache_hits += 1
        else:
            # Need to process this text
            texts_to_process.append(text)
            text_indices.append(i)
            cached_entities.append(None)  # Placeholder
    
    print(f"Cache hits: {cache_hits}/{len(all_text)} ({cache_hits/len(all_text)*100:.1f}%)")
    
    # DELETE CACHE FROM MEMORY TO SAVE RAM
    del cache
    print("Deleted cache from memory to save RAM")
    
    # Process uncached texts
    if texts_to_process:
        print(f"Processing {len(texts_to_process)} new texts")
        print("starting nlp")
        nlp = spacy.load("en_core_web_lg", disable=["textcat", "lemmatizer", "parser", "tagger", "attribute_ruler"])
        
        start = time()

        # note: initializing models costs me some time lol.
        processed_docs = list(tqdm(nlp.pipe(texts_to_process, batch_size=128, n_process=4), 
                                 total=len(texts_to_process), desc="Processing new texts", mininterval=1))
        processing_time = time() - start
        
        # Extract entities and store new cache entries
        for i, doc in enumerate(processed_docs):
            original_idx = text_indices[i]
            entities = extract_entities_from_doc(doc)
            cached_entities[original_idx] = entities
            
            # Store for cache update
            cache_key = get_cache_key(texts_to_process[i])
            new_cache_entries[cache_key] = entities
        
        print(f"Processing time: {processing_time}")
        
        # Update cache file with new entries
        print("Updating cache file...")
        if os.path.exists(cache_file):
            with open(cache_file, 'rb') as f:
                cache = pickle.load(f)
        else:
            cache = {}
        
        # Add new entries
        cache.update(new_cache_entries)
        
        # Save updated cache
        with open(cache_file, 'wb') as f:
            pickle.dump(cache, f)
        print(f"Updated cache now has {len(cache)} entries")
        
        # Clear cache from memory again
        del cache
        print("Cleared cache from memory again")
    else:
        print("All texts found in cache - no processing needed!")
    
    # Group entities back by document
    doc_entities = {}
    for entities, meta in zip(cached_entities, text_metadata):
        key = (meta['accession'], meta['path'])
        if key not in doc_entities:
            doc_entities[key] = {}
        
        # Merge entities from this text segment
        for entity_type, entity_list in entities.items():
            if entity_type not in doc_entities[key]:
                doc_entities[key][entity_type] = set()
            
            for entity in entity_list:
                doc_entities[key][entity_type].add(entity)
    
    # Build final records
    records = []
    for (accession, path), entities_by_type in doc_entities.items():
        # Convert sets to lists
        entities = {}
        for entity_type, entity_set in entities_by_type.items():
            entities[entity_type] = list(entity_set)
        
        records.append({
            'accession': accession,
            'path': path,
            'entities': entities
        })
    
    # Save
    with open(output_file, 'w', encoding='utf-8') as f:
        for record in records:
            json.dump(record, f, ensure_ascii=False)
            f.write('\n')
    
    print(f"Final cache statistics: {cache_hits} hits, {len(texts_to_process)} misses")
    print(f"Processed {len(all_text)} text segments from {len(records)} documents")


def main():
    # Create records directory if it doesn't exist
    os.makedirs("records", exist_ok=True)

    # Get all your batch files
    batch_files = glob.glob(r"8k_2024_parsed_text\portfolio_data_batch_*.jsonl")

    # Process each one
    for i, input_file in enumerate(batch_files):
        output_file = f"records/records_{i:04d}.jsonl"
        if os.path.exists(output_file):
            print(f"Skipping {output_file}")
            continue  # Skip if already exists
        print(f"Processing {input_file} -> {output_file}")
        extract_all_entities_from_jsonl(input_file, output_file)
        print(f"Completed batch {i+1}/{len(batch_files)}")

if __name__ == "__main__":
    main()