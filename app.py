from flask import Flask, request, jsonify, render_template
from Agent.agent import create_agent
from Agent.memory import save_user_memory
from Agent.intent import detect_intent
import os

from flask_cors import CORS

app = Flask(__name__)

CORS(app)

os.environ["OPENAI_API_KEY"]="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"

@app.route("/")
def home():
    return render_template("index.html")

@app.route("/conversation/", methods=["POST"])
def conversation():
    try:
        data = request.json
        user_input = data.get("input", "")
        user_id = data.get("user_id")

        if not user_id:
            return jsonify({"error": "User ID is required"}), 400

        # Fetch the agent for the user based on user_id
        agent = create_agent(user_id)

        # Fetch conversation history from memory
        memory = agent.memory

        # Determine the intent based on conversation history
        intent = detect_intent(user_input, memory)

        # Take action based on the intent
        if intent == "risk_analysis":
            response = agent.invoke(user_input)
            save_user_memory(user_id, user_input, response["output"])  # Save user-specific memory
            return jsonify({"intent": "risk_analysis", "response": response})

        elif intent == "investment_planning":
            response = agent.invoke(user_input)
            save_user_memory(user_id, user_input, response["output"])  # Save user-specific memory
            return jsonify({"intent": "investment_plan", "response": response})

        elif intent == "unknown":
            response = agent.invoke(user_input)
            save_user_memory(user_id, user_input, response["output"])  # Save user-specific memory
            return jsonify({"intent": "unknown", "response": response})

        return jsonify({"error": "Unable to determine intent"}), 400

    except Exception as e:
        return jsonify({"error": str(e)}), 500



if __name__ == "__main__":
    app.run(host="0.0.0.0", port=7001, debug=True)
