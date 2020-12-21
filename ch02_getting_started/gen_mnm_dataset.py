import sys
import random
import csv
from settings import DATA_DIRECTORY

def get_random_choice(lst):
  return random.choice(lst)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: gen_mnm_dataset number of entries", file=sys.stderr)
        sys.exit(-1)

    states = ["CA", "WA", "TX", "NV", "CO", "OR", "AZ", "WY", "NM", "UT"]
    colors = ["Brown", "Blue", "Orange", "Yellow", "Green", "Red"]
    fieldnames = ['State', 'Color', 'Count']


    num_entries = int(sys.argv[1])
    dataset = DATA_DIRECTORY + "/mnm_dataset.csv"

    with open(dataset, mode='w') as dataset_file:
        dataset_writer = csv.writer(dataset_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        dataset_writer.writerow(fieldnames)

        for i in range(num_entries):
            dataset_writer.writerow([get_random_choice(states), get_random_choice(colors), random.randint(10, 100)])

    print("Wrote %d lines in %s file" % (num_entries, dataset))
