FROM datamechanics/spark:3.1.1-hadoop-3.2.0-java-8-scala-2.12-python-3.8-latest

RUN pip3 install numpy
RUN pip3 install kafka_python
RUN pip3 install nltk && python3 -m nltk.downloader stopwords && python3 -m nltk.downloader wordnet
RUN pip3 install sklearn
RUN pip3 install flask
