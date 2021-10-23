#spark modules
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import isnan, when, count, col, split, udf, sum, max,concat


#python modules
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix,f1_score
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.preprocessing import LabelEncoder
import nltk
from nltk.stem import WordNetLemmatizer
nltk.download('wordnet')
stopwords = nltk.corpus.stopwords.words('english')
#import matplotlib.pyplot as plt
#import seaborn as sns
import pandas as pd
import numpy as np
import re
import warnings
import string
import pickle
from sklearn.pipeline import make_pipeline
#Stop words present in the library

from lime import lime_text
from sklearn.naive_bayes import MultinomialNB

warnings.filterwarnings("ignore")



spark = SparkSession.builder.config("spark.jars", "/usr/share/java/mysql-connector-java-8.0.25.jar") \
    .master("local").appName("PySpark_MySQL_test").getOrCreate()

df = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/sys") \
    .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "news") \
    .option("user", "root").option("password", "p@ssw0rd1").load()
df2=df.select("summary","title","topic").filter("topic is not  null and summary is not null").distinct();
df3=df2.filter("topic not like '%according%'")

wn = WordNetLemmatizer()
def text_preprocessing(review):
    review = re.sub('[^a-zA-Z]', ' ', review)
    review = review.lower()
    review = review.split()
    review = [wn.lemmatize(word) for word in review if not word in stopwords]
    #review= [i for i in review if i not in string.punctuation]
    #review= [i for i in review if i not in ]
    review = ' '.join(review)
    return review

text_preprocessing_udf = udf(lambda t: text_preprocessing(t),StringType())
df4=df3.select(concat("summary","title").alias("newstext"),"topic")

df5 = df4.select("newstext","topic").withColumn('news', text_preprocessing_udf(col("newstext")))
rawdf = df5.toPandas()

X = rawdf[['news']]
y = rawdf['topic']
encoder = LabelEncoder()
y = encoder.fit_transform(y)
x_train, x_test, y_train, y_test = train_test_split(X, y, test_size=0.20)
x_train=x_train.reset_index().drop(['index'],axis=1)
x_test=x_test.reset_index().drop(['index'],axis=1)
vectorizer = TfidfVectorizer()
x_trn_token = vectorizer.fit_transform(x_train['news'].values).toarray()
x_tst_token =vectorizer.transform(x_test['news'].values).toarray()
#clf = svm.SVC(kernel='linear', C=1, probability=True)
clf=MultinomialNB(alpha=.01)
clf.fit(x_trn_token, y_train )
pred = clf.predict(x_tst_token)

print(cross_val_score(clf,x_trn_token, y_train, cv=5, scoring="accuracy" ))
print(classification_report(y_test, pred))
print(f1_score(y_test, pred, average='weighted'))

pickle.dump(clf,open('NLPmodel.sav','wb'))
pickle.dump(vectorizer,open('vectorizer.sav','wb'))
pickle.dump(encoder,open('encoder.sav','wb'))

from lime import lime_text
from sklearn.pipeline import make_pipeline
c = make_pipeline(vectorizer, clf)
from lime.lime_text import LimeTextExplainer
explainer = LimeTextExplainer(class_names=encoder.classes_)
idx=8
class_names=encoder.classes_
exp = explainer.explain_instance(x_test['news'][idx], c.predict_proba, num_features=6, top_labels=2)
print(exp.available_labels())

