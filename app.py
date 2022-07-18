from flask import Flask, jsonify
from logic import *


app = Flask(__name__)


@app.route("/api/choropleth", methods=["GET", "POST"], defaults={'week': ''})
@app.route("/api/choropleth/<string:week>", methods=["GET", "POST"])
def choropleth(week):
    #Databases information read
    data = mortality_rates_by_departamento()

    actual = data[['codigo', 'actual']].rename(columns={'actual': 'mortalidad'})
    actual = actual.to_dict('records')

    predicted = data[['codigo', 'predicted']].rename(columns={'predicted': 'mortalidad'})
    predicted = predicted.to_dict('records')

    response = {'Actual': actual, 'Predicted': predicted}    

    return jsonify(response)

@app.route("/api/charts", methods=["GET", "POST"])
def charts():
    #Databases information read
    data = causal_impact_series()

    pre_post_index = data['pre_post_index'].tolist()
    pre_post = data['pre_post'].tolist()

    complete_preds_means = data['complete_preds_means'].tolist()
    complete_preds_lower = data['complete_preds_lower'].tolist()
    complete_preds_upper = data['complete_preds_upper'].tolist()

    point_effects_means = data['point_effects_means'].tolist()
    point_effects_lower = data['point_effects_lower'].tolist()
    point_effects_upper = data['point_effects_upper'].tolist()

    post_cum_effects_means = data['post_cum_effects_means'].tolist()
    post_cum_effects_lower = data['post_cum_effects_lower'].tolist()
    post_cum_effects_upper = data['post_cum_effects_upper'].tolist()

    response = {'pre_post_index': pre_post_index, 'pre_post': pre_post,
    'complete_preds_means': complete_preds_means, 'complete_preds_lower': complete_preds_lower, 'complete_preds_upper': complete_preds_upper,
    'point_effects_means': point_effects_means, 'point_effects_lower': point_effects_lower, 'point_effects_upper': point_effects_upper,
    'post_cum_effects_means': post_cum_effects_means, 'post_cum_effects_lower': post_cum_effects_lower, 'post_cum_effects_upper': post_cum_effects_upper}

    return jsonify(response)


if __name__ == '__main__':
    app.run(debug=True)

