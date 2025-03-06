# ai_decision.py
import openai
import json

def get_openai_response(system_instruction, prompt):
    try:
        response = openai.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_instruction},
                {"role": "user", "content": prompt}
            ],
            max_tokens=400
        )
        return json.loads(response.choices[0].message.content)
    except Exception as e:
        print(f"Error with OpenAI response: {e}")
        return []