import argparse
import openai
from langchain_openai import OpenAI

from configparser import ConfigParser
from pathlib import Path
import os

import re

def clean_text(text):
    """
    Cleans the generated text to remove any trailing incomplete sentence.

    Args:
    - text: The generated text.

    Returns:
    Cleaned text with no trailing incomplete sentence.
    """
    # Split the text into sentences using regex to match sentence-ending punctuation
    sentences = re.split(r'(?<=[.!?]) +', text)
    # Remove any trailing sentence that doesn't end with proper punctuation
    cleaned_sentences = [sentence for sentence in sentences if sentence.endswith(('.', '!', '?'))]
    # Join the cleaned sentences back into a single string
    cleaned_text = ' '.join(cleaned_sentences)
    return cleaned_text


# Setup OpenAI Key
def seed_openai_key(cfg: str="~/.cfg/openai.cfg"):
    """
    Reads OpenAI key from config file and adds it to environment.
    Assumed config location is "~/.cfg/openai.cfg"
    """
    # Get OpenAI Key
    config = ConfigParser()
    config.read(Path(cfg).expanduser())
    if 'API_KEY' not in config:
        raise ValueError(f"Could not read file at: {cfg}. Please ensure the config file exists and is correctly formatted.")
    openai_key = config['API_KEY']['secret']
    openai.api_key = openai_key
    os.environ['OPENAI_API_KEY'] = openai_key
    return openai_key

# Initialize the LangChain wrapper for OpenAI
llm = OpenAI(api_key=seed_openai_key())

def gen_reply(post_text, adjectives):
    """
    Generates a reply to a given political social media post, using specified adjectives.

    Args:
    - post_text: The text of the social media post to reply to.
    - adjectives: A list of adjectives to describe the desired reply style.

    Returns:
    A reply as a string.
    """
    # Adjust the prompt based on the presence of "agree" or "disagree"
    action_word = "to"
    if "agree" in adjectives:
        action_word = "agreeing with"
        adjectives.remove("agree")
    elif "disagree" in adjectives:
        action_word = "disagreeing with"
        adjectives.remove("disagree")

    adjectives_description = " ".join(adjectives)
    prompt = [f"Generate a {adjectives_description} reply {action_word} the political post: '{post_text}'."]
    response = llm.generate(prompt, max_tokens=60, temperature=0.7)
    
    generated_text = response.generations[0][0].text.strip()
    cleaned_reply = clean_text(generated_text)
    
    return cleaned_reply

def main():
    parser = argparse.ArgumentParser(description="Generate replies to political social media posts using specified adjectives.")
    parser.add_argument("post_text", type=str, help="The text of the social media post to reply to.")
    parser.add_argument("--adjectives", nargs='*', help="A list of adjectives to describe the reply. Can be specified multiple times.")

    args = parser.parse_args()
    if not args.adjectives:
        print("No adjectives specified. Exiting.")
        return

    reply = gen_reply(args.post_text, args.adjectives)
    print("\033[95mPost:", args.post_text, "\033[0m")
    print("\033[92mReply:", reply, "\033[0m")

if __name__ == "__main__":
    main()
