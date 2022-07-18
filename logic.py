import pandas as pd
from sqlalchemy.sql.expression import null
from causalimpact import CausalImpact
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from sqlalchemy.exc import SQLAlchemyError

#Database connection constants
HOST = "ds4a.c9wufjefyjfw.us-east-1.rds.amazonaws.com" 
PORT = 5432 
USER = "team92" 
PASSWORD = "finalproject" 
DATABASE = "ds4a" 

URL = f'postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}'

ENGINE = create_engine(URL)


def estimate_causal_impact():
    try:
        #Database information read
        data  = pd.read_sql('SELECT * FROM covid_colombia', ENGINE)
        data['contagiados'] = data['fallecidos'] + data['recuperados']
        data = data[['fallecidos', 'contagiados']][:-4] #Last 4 week are not taken into consideration due to the small data size

        #Causal Impact parameters
        pre_period = [0, 65]
        post_period = [66, len(data)-1]
        #Causal impact model execution
        model = CausalImpact(data, pre_period, post_period)

        #Model results

        #Effects
        #(Relative impact of the event and its 95% confidence interval)
        average_values = model.summary_data.average

        rel_effect = average_values['rel_effect']
        rel_effect_lower = average_values["rel_effect_lower"]
        rel_effect_upper = average_values["rel_effect_upper"]

        effects = {'rel_effect': rel_effect*100, 'rel_effect_lower': rel_effect_lower*100, 'rel_effect_upper': rel_effect_upper*100}

        #Series 
        #(Time series for actual data, predicted data, actual vs predicted differences, cummulative effect, and 95% confidence interval for each value)
        pre_post_index = [*model.pre_data.index.union(model.post_data.index)]
        pre_post = pd.concat([model.pre_data.iloc[:, 0], model.post_data.iloc[:, 0]])
        
        complete_preds_means = model.inferences['complete_preds_means']
        complete_preds_lower = model.inferences['complete_preds_lower']
        complete_preds_upper = model.inferences['complete_preds_upper']

        point_effects_means = model.inferences['point_effects_means']
        point_effects_lower = model.inferences['point_effects_lower']
        point_effects_upper = model.inferences['point_effects_upper']

        post_cum_effects_means = model.inferences['post_cum_effects_means']
        post_cum_effects_lower = model.inferences['post_cum_effects_lower']
        post_cum_effects_upper = model.inferences['post_cum_effects_upper']

        series = pd.DataFrame({'pre_post_index': pre_post_index, 'pre_post': pre_post, 
        'complete_preds_means': complete_preds_means, 'complete_preds_lower': complete_preds_lower, 'complete_preds_upper': complete_preds_upper, 
        'point_effects_means': point_effects_means, 'point_effects_lower': point_effects_lower, 'point_effects_upper': point_effects_upper, 
        'post_cum_effects_means': post_cum_effects_means, 'post_cum_effects_lower': post_cum_effects_lower, 'post_cum_effects_upper': post_cum_effects_upper}).fillna(0)
        
        #series = series_df.T.to_dict().values()


        '''
        series = {'pre_post_index': pre_post_index, 'pre_post': pre_post,
         'complete_preds_means': complete_preds_means, 'complete_preds_lower': complete_preds_lower, 'complete_preds_upper': complete_preds_upper,
         'point_effects_means': point_effects_means, 'point_effects_lower': point_effects_lower, 'point_effects_upper': point_effects_upper,
         'post_cum_effects_means': post_cum_effects_means, 'post_cum_effects_lower': post_cum_effects_lower, 'post_cum_effects_upper': post_cum_effects_upper}
        '''
        #series =  list(series_df.itertuples(index=False, name=None))


        return (effects, series)
    except:
        return None

def update_causal_impact_tables():
    response = estimate_causal_impact()
    if not response:
        return None

    effect, series = response

    with ENGINE.begin() as con:
        try:
            #Updates causal_impact_effects table
            effect_statement = text("""INSERT INTO causal_impact_effects (rel_effect, rel_effect_lower, rel_effect_upper) VALUES(:rel_effect, :rel_effect_lower, :rel_effect_upper)""")
            con.execute(effect_statement, **effect)

            #Updates causal_impact_series table
            delete_series_statement = text("""DELETE FROM causal_impact_series""") #'With Engine.begin() as con' works as a SQL transaction, so the DELETE query will be not executed if the INSERT is not executed too
            con.execute(delete_series_statement)
            series.to_sql('causal_impact_series', ENGINE, index=False, if_exists='append')
        except SQLAlchemyError as e:
            error = str(e.__dict__['orig'])
            print(error)

def causal_impact_effects():
    try:
        #Database information read
        data  = pd.read_sql('SELECT * FROM causal_impact_effects WHERE id = (SELECT MAX(id) FROM causal_impact_effects)', ENGINE)
        return data.iloc[0]
    except:
            return None

def causal_impact_series():
    try:
        #Database information read
        data  = pd.read_sql('SELECT * FROM causal_impact_series', ENGINE)
        return data
    except:
        return None


def mortality_rates_by_departamento(week = "2021-07-25"):
    try:
        #Databases information read
        data = pd.read_sql(f"SELECT dep.codigo, cod.fallecidos, cod.recuperados, cod.acumulado, dep.poblacion FROM covid_departamentos cod JOIN departamentos dep ON cod.codigo = dep.codigo WHERE semana = '{week}'".format(week=week), ENGINE)
        effects = causal_impact_effects()

        #Actual mortality rate
        data['actual'] = 100*data['fallecidos']/(data['fallecidos'] + data['recuperados'])

        #Variables required for predicted mortality rate
        data['vac_percentage'] = data['acumulado']/(2*data['poblacion'])
        data['normalized_vac'] = (data['vac_percentage']-data['vac_percentage'].min())/(data['vac_percentage'].max()-data['vac_percentage'].min())

        #Max and min effect values
        rel_effect_lower = effects['rel_effect_lower']
        rel_effect_upper = effects['rel_effect_upper']

        interval_length = rel_effect_upper - rel_effect_lower

        #Effect to be applied to actual mortality rate
        data['effect'] = 1 - (rel_effect_upper - interval_length*data['normalized_vac'])/100

        #Predicted mortality rate
        data['predicted'] = data['actual']*data['effect']

        columns = ['codigo', 'actual', 'predicted']

        return data[columns]
    except:
        return None
