from mrjob.job import MRJob
import csv
from collections import defaultdict

class MostFrequentPhoneNumbers(MRJob):

    def mapper(self, _, line):
        row = next(csv.reader([line]))  # Parse CSV line
        if len(row) >= 17:  # Ensure row has at least 17 columns
            locality = row[16].strip()  # Extract locality
            phone_str = row[7].strip()  # Extract phone number
            if phone_str and phone_str.isdigit():  # Check if phone number is not null and numeric
                yield locality, phone_str

    def combiner(self, locality, phone_numbers):
        phone_counts = defaultdict(int)
        for phone in phone_numbers:
            phone_counts[phone] += 1
        most_frequent_phone = max(phone_counts, key=phone_counts.get)
        yield locality, most_frequent_phone

    def reducer(self, locality, phone_numbers):
        phone_counts = defaultdict(int)
        for phone in phone_numbers:
            phone_counts[phone] += 1
        most_frequent_phone = max(phone_counts, key=phone_counts.get)
        yield locality, most_frequent_phone

if __name__ == '__main__':
    MostFrequentPhoneNumbers.run()