# ai_decision.py
import openai
import json
import os

openai.api_key = os.getenv("OPENAI_KEY")

def get_openai_response(system_instruction, prompt):
    try:
        # Call the OpenAI API using the new interface
        response = openai.chat.completions.create(
            model="gpt-4o-mini",  # Use the appropriate model
            messages=[
                {"role": "system", "content": system_instruction},
                {"role": "user", "content": prompt}
            ],
            max_tokens=400  # Adjust the number of tokens as needed
        )
        # Extract the text from the response
        response = response.choices[0].message.content
        #actions = json.loads(response)
        return response
    except Exception as e:
        return f"An error occurred: {e}"