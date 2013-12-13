import numpy as np

import requests
from pattern import web

from mrjob.job import MRJob
from itertools import combinations, permutations

def get_synonyms(word):
	  word = word.strip()
	  
		# query the word
	  url = 'http://thesaurus.com/browse/%s' % word
	  data = requests.get(url).text
	  data = web.Element(data)
	  
	  # check if no results
	  if len(data('#words-gallery-no-results')) > 0:
	    return []
	  
	  # check if no synonyms
	  if len(data('.relevancy-list')) == 0:
	    return []
	  
	  # get and return synonyms of word
	  words = data.by_class('relevancy-list')[0]
	  words = [word.content for word in words('span.text')]
	  return words


class SynonymSimilarities(MRJob):

    def steps(self):
        "the steps in the map-reduce process"
        thesteps = [
            self.mr(mapper=self.pair_synonyms_mapper, reducer=self.calc_synonyms_collector),
            self.mr(mapper=self.expand_synonyms_mapper, reducer=self.expand_synonyms_collector)
        ]
        return thesteps

    def pair_synonyms_mapper(self,_,line):
        """
        take all combinations of the word's synonyms and yield as key the pair
        id, and as value the original word (which will later be discarded)
        """
        # yield synonym pairs
        word = line.strip()
        synonyms=get_synonyms(word)
        synonyms.append(word)
        for x in combinations(synonyms,2):
            if x[0] < x[1]:
                yield (x[0],x[1]) , word
            else:
                yield (x[1],x[0]) , word
                
        # also yield the synonyms, to be counted later
        for synonym in synonyms:
            yield synonym , 1

    def calc_synonyms_collector(self, key, values):
        """
        Pick up the information from the previous yield as shown. Compute
        the "similarity" of these two words as the number of times they
        both appear as synonyms for another word, and also yield the number
        of times that a particular word appears as a synonym of any words
        """
        yield key , len(list(values))

    def expand_synonyms_mapper(self, key, values):
        """
        take each pair and output each word in the key as its own entry (when collected
        and reloaded, this will make it easier to query synonyms later)
        """
        if isinstance(key,unicode) or isinstance(key,str):
            yield key, values             # passes along the number of times a synonym appears
        else:
            yield key[0], (key[1],values) # passes the first word's relationship to the second
            yield key[1], (key[0],values) # passes the second word's relationship to the first

    def expand_synonyms_collector(self, key, values):
        """
        Collect the synonyms for each word and output
        """
        # find the total appearances of a word and each of its related synonyms
        total_appearances = 0
        synonym_values = []
        for value in list(values):
            if isinstance(value, int):
                total_appearances = value
            else:
                synonym_values.append(value)
        
        # normalize a word's relationship by dividing by the number of times it is seen in total
        normalized_values = {}
        for synonym_value in synonym_values:
            normalized_values[synonym_value[0]] = float(synonym_value[1])/total_appearances
        yield key , normalized_values

#Below MUST be there for things to work
if __name__ == '__main__':
    SynonymSimilarities.run()