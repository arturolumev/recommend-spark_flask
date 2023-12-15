#Load all the required library
from pyspark.sql import SparkSession
from pathlib import Path
from datetime import datetime
from pyspark.sql.functions import col, collect_list, udf, struct, explode
from pyspark.sql.types import DoubleType
from math import sqrt
import json
import http.client
import requests
import sys
import pandas
import os
import shutil
import math

from flask import Flask, jsonify

# Crear una instancia de Flask en Spark
app = Flask(__name__)

spark = SparkSession.builder.appName("Recomendaciones") \
                .config("spark.executor.memory", "4g") \
                .config("spark.driver.memory", "4g") \
                .getOrCreate()
                
df = spark.read.csv('ratings.csv', header=True, inferSchema=True).select('userId', 'movieId', 'rating')
df = df.repartition(10)
df.cache()

grouped_df = df.groupBy('userId').agg(collect_list('movieId').alias('movieIds'), collect_list('rating').alias('ratings'))
user_dict = grouped_df.rdd.map(lambda row: (row.userId, dict(zip(row.movieIds, row.ratings)))).collectAsMap()
print("Diccionario completo")

def manhattan_distance(user1, user2):
    common_elements = set(user1.keys()) & set(user2.keys())
    distance = sum(abs(user1[k] - user2[k]) for k in common_elements)
    return distance

def nearest_neighbors(user_id, N=10):
    user = user_dict.get(user_id)
    if user is None:
        return []

    distances = [(other_user_id, manhattan_distance(user, user_dict[other_user_id])) for other_user_id in user_dict if other_user_id != user_id]
    nearest = sorted(distances, key=lambda x: x[1])[:N]
    return nearest

def recommendations_for_user(user_id, neighbors):
    user = user_dict.get(user_id)
    if user is None or not neighbors:
        return []

    unrated_movies = [movie_id for movie_id in set(user_dict[user_id].keys()) if user_dict[user_id][movie_id] < 0]
    
    recommendations = []
    for movie_id in unrated_movies:
        weighted_sum = 0
        weight_sum = 0
        for neighbor_id, distance in neighbors:
            neighbor_rating = user_dict[neighbor_id].get(movie_id)
            if neighbor_rating is not None:
                weight = 1 / (distance + 1)
                weighted_sum += neighbor_rating * weight
                weight_sum += weight
        if weight_sum > 0:
            recommendations.append((movie_id, weighted_sum / weight_sum))
    
    recommendations = sorted(recommendations, key=lambda x: x[1], reverse=True)
    
    # Si no hay recomendaciones, devolver algunas películas mejor valoradas por los vecinos
    if not recommendations:
        top_rated_movies = sorted(user_dict[user_id].items(), key=lambda x: x[1], reverse=True)[:10]
        recommendations = [(movie_id, rating) for movie_id, rating in top_rated_movies]
    
    return recommendations

# Almacenar el SparkSession y las funciones relevantes en una variable global
spark_context = {
    'spark_session': spark,
    'user_dict': user_dict,
    'manhattan_distance': manhattan_distance,
    'nearest_neighbors': nearest_neighbors,
    'recommendations_for_user': recommendations_for_user
}

# Definir una ruta para las recomendaciones basadas en el user_id
@app.route('/recommendations/<int:user_id>', methods=['GET'])
def get_recommendations(user_id):
    # Acceder a las funciones de recomendación que ya tienes implementadas en Spark
    # Esto podría ser tu lógica actual de recomendación basada en user_id
    recommendations = spark_context['recommendations_for_user'](user_id, spark_context['nearest_neighbors'](user_id))
    
    # Devolver las recomendaciones como JSON
    return jsonify({"recommendations": recommendations})

if __name__ == '__main__':
    # Ejecutar el servicio Flask en Spark en el puerto 8888
    app.run(host='0.0.0.0', port=8888)
