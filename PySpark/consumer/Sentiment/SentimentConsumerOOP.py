from Consumer import Consumer

from pyspark.sql import Row

import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
nltk.download('vader_lexicon')
sia = SentimentIntensityAnalyzer()

class SentimentConsumer(Consumer):
    
    def __init__(self, label) -> None:
        super().__init__()
        self.label = label

    def predict(self, data: Row) -> Row:
        """
        Return the polarity of the sentiment, not taking into account neutral.
        """
        if data.text == None:
            return self.toRow(data, "NOT APPLYABLE")
        if sia.polarity_scores(data.text)['compound'] > 0:
            return self.toRow(data, "positive")
        
        return self.toRow(data, "negative")

if __name__ == "__main__":
    consumer = SentimentConsumer("VADER")

    consumer.start()