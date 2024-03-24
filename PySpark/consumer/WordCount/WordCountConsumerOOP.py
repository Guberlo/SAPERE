from Consumer import Consumer
from pyspark.sql import Row

import json

import nltk
nltk.download('stopwords')

from nltk.corpus import stopwords
stopw = set(stopwords.words('english'))

class WordCountConsumer(Consumer):

    def __init__(self, label) -> None:
        super().__init__()
        self.label = label

    def predict(self, data: Row) -> Row:
        """
            Count the occurences of each word in the passed text.
            The prediction is returned as a JSON with {'word': number_of_occurences}.

            Return a Row containing {id: Elastic_Search_id, type: Job_Label, text, prediction: result_of_the_prediction}.
        """
        tokens = [t for t in data.text.split()]
        clean_tokens = tokens[:]

        for token in tokens:
            if token in stopw:
                clean_tokens.remove(token)

        values = []
        freq = nltk.FreqDist(clean_tokens)

        for key,val in freq.items():
            values.append({ key: val })
        
        return self.toRow(data, json.dumps(values))

if __name__ == "__main__":
    wordConsumer = WordCountConsumer("WordCount")

    wordConsumer.start()