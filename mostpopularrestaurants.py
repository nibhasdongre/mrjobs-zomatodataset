from mrjob.job import MRJob
import csv
from collections import defaultdict

class Top10PopularRestaurants(MRJob):

    def mapper(self, _, line):
        row = next(csv.reader([line]))  # Parse CSV line
        if len(row) >= 17:  # Ensure row has at least 17 columns
            name = row[2].strip()  # Extract restaurant name
            votes_str = row[6].strip()  # Extract votes
            if votes_str and votes_str.isdigit():  # Check if votes are not null and numeric
                yield name, int(votes_str)

    def combiner(self, name, votes):
        total_votes = sum(votes)
        yield None, (total_votes, name)

    def reducer_init(self):
        self.top_restaurants = []

    def reducer(self, _, votes_and_names):
        for total_votes, name in votes_and_names:
            self.top_restaurants.append((total_votes, name))
            self.top_restaurants.sort(reverse=True)
            self.top_restaurants = self.top_restaurants[:10]

    def reducer_final(self):
        for total_votes, name in self.top_restaurants:
            yield name, total_votes

if __name__ == '__main__':
    print("The top restaurants based on votes are : ")
    Top10PopularRestaurants.run()
