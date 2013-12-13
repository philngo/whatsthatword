import numpy as np

import json

from mrjob.job import MRJob
from itertools import combinations, permutations

from scipy.stats.stats import pearsonr


class SynonymSimilarities(MRJob):

    def steps(self):
        "the steps in the map-reduce process"
        thesteps = [
            self.mr(mapper=self.pair_synonyms_mapper, reducer=self.calc_synonyms_collector)
        ]
        return thesteps

    def pair_synonyms_mapper(self,_,line):
        """
        take all combinations of the word's synonyms and yield as key the pair
        id, and as value the original word (which will later be discarded)
        """
        data = [json.loads(j) for j in line.strip().split("\t")]
        yield data[0][0], (data[0][1],data[1])
        yield data[0][1], (data[0][0],data[1])

    def calc_synonyms_collector(self, key, values):
        """
        Pick up the information from the previous yield as shown. Compute
        the "similarity" of these two words as the number of times they
        both appear as synonyms for another word
        """
        yield key , [x for x in values]

#Below MUST be there for things to work
if __name__ == '__main__':
    SynonymSimilarities.run()