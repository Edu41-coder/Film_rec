from flask import Flask, jsonify
import subprocess
app = Flask(__name__)
@app.route('/train', methods=['POST'])
def train():
    try:
        # Exécuter le processus d'entraînement
        subprocess.run(['dvc', 'repro'], check=True)
        return jsonify({"message": "Entraînement et évaluation terminés avec succès."}), 200
    except subprocess.CalledProcessError as e:
        return jsonify({"error": str(e)}), 500
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)














