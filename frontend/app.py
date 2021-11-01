from flask import Flask, jsonify, request, session, redirect, url_for, g, Response
from flask.templating import render_template
import requests
import json
import os
from datetime import datetime


app = Flask(__name__)
#SECRET_KEY = os.urandom(24)

query = {}
global accu , model, dt
accu , model ,dt = "","",""
@app.route('/', methods = ['GET'])
def home():
    return render_template('home.html', message = "Click on the links to go the respective pages")


@app.route('/about', methods = ["GET"])
def about():
    return render_template('about.html')

    
@app.route('/predict', methods =['GET', 'POST'])
def predict():
    exp,predict = "",""
    qry={}
    if request.method == 'GET':
        message = "Let us predict"
        return render_template('predict.html', message = message)
    else:
        news = request.form['entry'] 
        print(news)
        qry['query']=news
        predict = requests.post("http://predictor:3000/predict",json=qry)
        result=predict.json()
        print(result)
        try:  
            exp=result['exp'] 
        except:  
            exp="a"
        return render_template('predict.html', entry=news,predict=result['prediction'], exp=exp)


@app.route('/retrain',  methods =['GET', 'POST'])
def Retrain():
    global accu , model , dt
    if request.method == 'GET':
        return render_template('retrain.html',accu=accu, algo=model,dt=dt)
    else:
        retrain=requests.get("http://trainer:3000/train?update=true")
        result=retrain.json()
        accu=result['Naive_Bayes']
        model='Naive_Bayes'
        dt=str(datetime.now())
        return render_template('retrain.html',accu=accu, algo=model,dt=dt) 


# if __name__ == '__main__':  
    #app.secret_key = SECRET_KEY
app.run(host='0.0.0.0', port=5000)
