import csv
from datetime import datetime
from pymongo import MongoClient, InsertOne, UpdateOne
import os
import json
from typing import List, Dict, Any

# MongoDB connection details
client = MongoClient('mongodb+srv://chora-club:VIsYV9nBcTpTDpme@choraclub.2uqnfpc.mongodb.net/')
# client = MongoClient('mongodb+srv://captainwalter11:GAd5lSC2kV2vVTZw@cluster0.qkgxzxw.mongodb.net/')
db = client['CPI']
collection = db['delegate_data']
checkpoint_collection = db['import_checkpoints']

class ImportCheckpoint:
    def __init__(self):
        self.checkpoint_collection = checkpoint_collection
        
    def get_last_checkpoint(self) -> Dict[str, Any]:
        # First, check for any incomplete files
        incomplete_checkpoint = self.checkpoint_collection.find_one(
            {'status': {'$ne': 'completed'}},
            sort=[('timestamp', -1)]
        )
        if incomplete_checkpoint:
            return incomplete_checkpoint
            
        # If no incomplete files, get the last completed file
        return self.checkpoint_collection.find_one(
            {'status': 'completed'},
            sort=[('timestamp', -1)]
        ) or {'processed_files': [], 'last_file': None, 'last_position': 0}

    def get_verified_position(self, file_name: str) -> int:
        """Verify the actual last processed position by counting records in MongoDB"""
        date_str = file_name.replace('.csv', '')
        date = datetime.strptime(date_str, '%Y-%m-%d')
        
        # Count actual stored records
        stored_count = collection.count_documents({'date': date})
        
        # Get the claimed checkpoint position
        checkpoint = self.checkpoint_collection.find_one({'file_name': file_name})
        claimed_position = checkpoint.get('position', 0) if checkpoint else 0
        
        # If stored count is less than claimed position, return stored count
        if stored_count < claimed_position:
            print(f"Found inconsistency in {file_name}: Claimed position {claimed_position} but only {stored_count} records stored")
            # Update checkpoint to reflect reality
            self.update_checkpoint(file_name, stored_count, self.get_total_records(file_name))
            return stored_count
            
        return claimed_position

    def get_total_records(self, file_path: str) -> int:
        """Count total records in CSV file"""
        with open(file_path, 'r') as csvfile:
            return sum(1 for _ in csv.DictReader(csvfile))
    
    def update_checkpoint(self, file_name: str, position: int, total_records: int):
        checkpoint = {
            'timestamp': datetime.now().replace(microsecond=0),
            'file_name': file_name,
            'position': position,
            'total_records': total_records,
            'status': 'in_progress'
        }
        self.checkpoint_collection.update_one(
            {'file_name': file_name},
            {'$set': checkpoint},
            upsert=True
        )
    
    def mark_file_complete(self, file_name: str):
        self.checkpoint_collection.update_one(
            {'file_name': file_name},
            {'$set': {
                'timestamp': datetime.now().replace(microsecond=0),
                'status': 'completed',
                'completion_time': datetime.now()
            }},
            upsert=True
        )
        
    def verify_file_completion(self, file_name: str) -> bool:
        """Verify if all records from the file have been properly processed"""
        date_str = file_name.replace('.csv', '')
        date = datetime.strptime(date_str, '%Y-%m-%d')
        
        stored_records = collection.count_documents({'date': date})
        
        # Get the total records from checkpoint
        checkpoint = self.checkpoint_collection.find_one({'file_name': file_name})
        if not checkpoint or 'total_records' not in checkpoint:
            return False
            
        return stored_records >= checkpoint['total_records']

def ensure_indexes():
    """Ensure the required indexes exist, dropping and recreating if necessary."""
    try:
        # Drop existing indexes except _id
        collection.drop_indexes()
        print("Dropped existing indexes")
        
        # Create new index
        collection.create_index(
            [("delegate_id", 1), ("date", 1)], 
            unique=True,
            background=True
        )
        print("Created new index on delegate_id and date")
    except Exception as e:
        print(f"Error managing indexes: {e}")
        raise

def process_csv_files(directory_path: str):
    ensure_indexes()
    
    checkpoint_manager = ImportCheckpoint()
    files = [f for f in os.listdir(directory_path) if f.endswith('.csv')]
    files.sort()
    
    # Get the last checkpoint
    last_checkpoint = checkpoint_manager.get_last_checkpoint()
    
    # If there's an incomplete file, start from there
    if last_checkpoint.get('status') == 'in_progress':
        start_file = last_checkpoint.get('file_name')
        if start_file in files:
            start_idx = files.index(start_file)
            print(f"Resuming from incomplete file: {start_file}")
        else:
            start_idx = 0
    else:
        # If the last file was completed, start from the next file
        last_file = last_checkpoint.get('file_name')
        if last_file in files:
            start_idx = files.index(last_file) + 1
        else:
            start_idx = 0
    
    total_files = len(files)
    print(f"Starting processing from file {start_idx + 1} of {total_files}")
    
    for file_count, file_name in enumerate(files[start_idx:], start=start_idx + 1):
        file_path = os.path.join(directory_path, file_name)
        
        # Get verified position based on actual stored records
        start_position = checkpoint_manager.get_verified_position(file_name)
        total_records = checkpoint_manager.get_total_records(file_path)
        
        print(f"Processing {file_name}: Starting from position {start_position} of {total_records}")
        
        try:
            while start_position < total_records:
                json_data = convert_csv_to_json(file_path, start_position, checkpoint_manager)
                if json_data:
                    insert_json_to_mongo(json_data, file_name, file_count)
                    start_position += len(json_data)
                else:
                    break  # No more data to process in this file
                
            # Verify completion after processing all batches
            if checkpoint_manager.verify_file_completion(file_name):
                checkpoint_manager.mark_file_complete(file_name)
                print(f"Successfully completed file: {file_name}")
            else:
                print(f"File {file_name} needs reprocessing")
                break  # Stop processing and don't move to the next file
            
        except Exception as e:
            print(f"Error processing file {file_name}: {e}")
            break  # Stop processing and don't move to the next file

def convert_csv_to_json(file_path: str, start_position: int, checkpoint_manager: ImportCheckpoint) -> List[Dict]:
    json_data = []
    
    with open(file_path, mode='r') as csvfile:
        csv_reader = csv.DictReader(csvfile)
        file_name = os.path.basename(file_path)
        total_records = checkpoint_manager.get_total_records(file_path)
        
        # Skip to the verified start position
        for _ in range(start_position):
            next(csv_reader, None)
        
        # Extract date from file name
        date_str = file_name.replace('.csv', '')
        date = datetime.strptime(date_str, '%Y-%m-%d')
        
        for position, row in enumerate(csv_reader, start=start_position):
            delegate_id = row['delegate']
            voting_power = {}

            # Process regular voting power fields
            if 'voting_power' in row:
                vp_value = try_parse_float(row['voting_power'])
                if vp_value is not None:
                    voting_power['vp'] = vp_value

            # Process th_vp field
            if 'th_vp' in row:
                vp_th = try_parse_float(row['th_vp'])
                if vp_th is not None:
                    voting_power['th_vp'] = vp_th

            # Houses and their possible variations
            houses = ['ch', 'gc', 'sc', 'coc', 'dab']
            
            # Process regular round/season fields
            for house in houses:
                # Process round fields (r2 to r6)
                for i in range(2, 7):
                    member_col_r = f'{house}_member_r{i}'
                    vp_col_r = f'{house}_vp_r{i}'
                    
                    if member_col_r in row and row.get(member_col_r) == '1':
                        vp_value = try_parse_float(row.get(vp_col_r))
                        if vp_value is not None:
                            voting_power[vp_col_r] = vp_value

                # Process season fields (s3 to s6)
                for i in range(3, 7):
                    # Regular season fields
                    member_col_s = f'{house}_member_s{i}'
                    vp_col_s = f'{house}_vp_s{i}'
                    
                    if member_col_s in row and row.get(member_col_s) == '1':
                        vp_value = try_parse_float(row.get(vp_col_s))
                        if vp_value is not None:
                            voting_power[vp_col_s] = vp_value

                    # Special MM (market maker) fields for gc
                    if house == 'gc':
                        member_col_mm = f'{house}_member_mm_s{i}'
                        vp_col_mm = f'{house}_vp_mm_s{i}'
                        
                        if member_col_mm in row and row.get(member_col_mm) == '1':
                            vp_value = try_parse_float(row.get(vp_col_mm))
                            if vp_value is not None:
                                voting_power[vp_col_mm] = vp_value

            if voting_power:
                json_data.append({
                    'delegate_id': delegate_id,
                    'date': date,
                    'voting_power': voting_power
                })
            
            # Update checkpoint more frequently (every 500 records)
            if position % 500 == 0:
                stored_count = collection.count_documents({'date': date})
                if stored_count < position:
                    print(f"Warning: Only {stored_count} records stored but processing position {position}")
                    return json_data
                
                checkpoint_manager.update_checkpoint(file_name, position, total_records)
                print(f"Verified checkpoint update for {file_name}: {position}/{total_records}")

            if len(json_data) >= 500:
                return json_data

    return json_data

def insert_json_to_mongo(json_data: List[Dict], file_name: str, file_count: int):
    if not json_data:
        print(f"No data to insert for file {file_name}")
        return
        
    try:
        # Prepare upsert operations to avoid duplicates
        operations = [
            UpdateOne(
                {
                    'delegate_id': record['delegate_id'],
                    'date': record['date']
                },
                {'$set': record},
                upsert=True
            ) for record in json_data
        ]
        
        result = collection.bulk_write(operations, ordered=False)
        print(f"Processed {len(operations)} records from '{file_name}'")
        print(f"Upserted: {result.upserted_count}, Modified: {result.modified_count}")
        
    except Exception as e:
        print(f"An error occurred during bulk write for file '{file_name}': {e}")
        raise

def try_parse_float(value):
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

if __name__ == "__main__":
    csv_directory = '.'
    try:
        process_csv_files(csv_directory)
    except KeyboardInterrupt:
        print("\nScript interrupted by user. Progress has been saved in checkpoints.")
    except Exception as e:
        print(f"An error occurred: {e}")
        raise