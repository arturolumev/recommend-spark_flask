from flask import Flask, jsonify
import requests

app = Flask(__name__)

@app.route('/')
def hello_world():
    return 'Hello, World!'

# Esta ruta manejará las solicitudes GET a /get_recommendations/<int:user_id>
@app.route('/api/<int:user_id>', methods=['GET'])
def get_recommendations(user_id):
    spark_app_url = 'http://spark_app:8888'  # URL del contenedor Spark

    # Hacer una solicitud al contenedor Spark para obtener recomendaciones
    spark_response = requests.get(f'{spark_app_url}/recommendations/{user_id}')

    # Manejar la respuesta de Spark y enviar la respuesta a la solicitud de Flask
    if spark_response.status_code == 200:
        return spark_response.json(), 200
    else:
        return jsonify({"message": "Error al obtener recomendaciones"}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
