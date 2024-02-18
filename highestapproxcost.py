from mrjob.job import MRJob
from mrjob.step import MRStep
import csv
import sys

class MRHighestApproxCost(MRJob):

    def configure_args(self):
        super(MRHighestApproxCost, self).configure_args()
        self.add_passthru_arg('--field-limit', default=sys.maxsize, type=int, help='CSV field size limit')

    def mapper_init(self):
        csv.field_size_limit(self.options.field_limit)

    def mapper(self, _, line):
        row = next(csv.reader([line]))
        if len(row) >= 13:  # Ensure row has at least 13 columns
            name = row[2]
            approx_cost_str = row[12].replace(',', '')  # Considering approx_cost(for two people) is at index 12
            if approx_cost_str.isdigit():
                approx_cost = int(approx_cost_str)
                yield None, (name, approx_cost)

    def reducer_init(self):
        self.restaurants = {}

    def reducer(self, _, values):
        for name, cost in values:
            if name not in self.restaurants:
                self.restaurants[name] = cost
            else:
                if cost > self.restaurants[name]:
                    self.restaurants[name] = cost

    def reducer_final(self):
        sorted_restaurants = sorted(self.restaurants.items(), key=lambda x: x[1], reverse=True)
        for i in range(min(5, len(sorted_restaurants))):
            yield sorted_restaurants[i][0], sorted_restaurants[i][1]

    def steps(self):
        return [
            MRStep(mapper_init=self.mapper_init,
                   mapper=self.mapper,
                   reducer_init=self.reducer_init,
                   reducer=self.reducer,
                   reducer_final=self.reducer_final)
        ]

if __name__== '__main__':
    MRHighestApproxCost.run()
