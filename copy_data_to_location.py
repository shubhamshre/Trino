import os
import shutil

csv_folder = "/Users/se/Documents/Programming/dag/pontus_golden_data/data"
final_folder = "/Users/se/Documents/Programming/Trino/data"

for filename in os.listdir(csv_folder):
    if filename.endswith(".csv"):
        dest_path = os.path.join(final_folder, filename[:-4])
        try:
            os.makedirs(dest_path)
        except:
            None
        source_file = os.path.join(csv_folder, filename)
        destination_file = os.path.join(dest_path, filename)
        shutil.copy(source_file, destination_file)
