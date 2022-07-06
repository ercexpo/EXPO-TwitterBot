import response
import random

response_templates, news_templates, sports_df, entertainment_df, lifestyle_df = response.load_all_files()

tokenizer, model = response.load_model()

tweets = [
    "Dave Roberts is the Dodgers' biggest opponent. Never goes with his gut. https://t.co/gv1Np3NUZZ",
    "That encanto song https://t.co/fcmSQ65CPJ",
    "Celtics coming in as underdogs on DK Sportbooks? Keep overlooking them...",
    "I hate the ps4 it’s a shit box smh I need a pc or a ps5",
    "Real Madrid has to hang in",
]


generated_responses = response.run_model(tweets, tokenizer, model, response_templates)

print(generated_responses)

final_outputs = []
for gen_resp in generated_responses:
    topic = random.choice(['sports', 'entertainment', 'lifestyle'])
    final_outputs.append(response.append_url(topic, gen_resp, news_templates, sports_df, entertainment_df, lifestyle_df))
print(final_outputs)
