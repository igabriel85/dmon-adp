from flask import Flask
from flask.ext.restplus import Api

app = Flask("dmon-adp")
api = Api(app, version='0.0.1', title='DICE Anomaly Detection Platform API',
          description="RESTful API for the DICE Anomaly Detection Platform  (dmon-adp)",
          )


# changes the descriptor on the Swagger WUI and appends to api /dmon and then /v1
adp = api.namespace('adp', description='dmon anomaly detection operations')