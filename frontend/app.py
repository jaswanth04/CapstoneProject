from flask import Flask, jsonify, request, session, redirect, url_for, g, Response
from flask.templating import render_template
#from ml_utils import predict_credit, retrain, explain_model
import sys
import io
import nltk
import pickle
import os
import numpy as np
from nltk.stem import WordNetLemmatizer
#nltk.download('wordnet')
import re
from lime import lime_text
from sklearn.pipeline import make_pipeline
from lime.lime_text import LimeTextExplainer


#-------------TBD
NLPmodel=pickle.load(open('NLPmodel.sav','rb'))
vectorizer=pickle.load(open('vectorizer.sav','rb'))
encoder=pickle.load(open('encoder.sav','rb'))
c = make_pipeline(vectorizer, NLPmodel)
#-----------------
app = Flask(__name__)
SECRET_KEY = os.urandom(24)

query = {}

@app.route('/', methods = ['GET'])
def home():
    return render_template('index.html', message = "Click on the links to go the respective pages")


@app.route('/about', methods = ["GET"])
def about():
    return render_template('about.html')

    
@app.route('/predict', methods =['GET', 'POST'])
def predict():
    exp,predict = "",""
    if request.method == 'GET':
        message = "Let us predict"
        return render_template('predict.html', message = message)
    else:
        news = request.form['entry'] 
        print(news)
        predict = 'business'
        explainer = LimeTextExplainer(class_names=encoder.classes_)
        class_names=encoder.classes_
        exp = explainer.explain_instance(news, c.predict_proba, num_features=len(encoder.classes_), top_labels=2)
        print(exp.available_labels())
        exp = exp.as_html()
        return render_template('predict.html', predict=predict, exp=exp)


@app.route('/retrain',  methods =['GET', 'POST'])
def Retrain():

    if request.method == 'GET':
        return render_template('retrain.html')
    else:
        accu=86
        algo='Logistic Regression'

        return render_template('retrain.html',accu=accu, algo=algo) 


if __name__ == '__main__':  
    app.secret_key = SECRET_KEY
    app.run(port=5000, debug=True)