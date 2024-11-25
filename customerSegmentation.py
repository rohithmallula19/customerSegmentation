# %%
# Import necessary libraries
import pandas as pd
import numpy as np
import openai
from openai import OpenAI
import json
import ast
import pandas as pd
import numpy as np
import os
import pydash as pyd
# %%
# Azure OpenAI GPT 3.5 variables
client = OpenAI(
    api_key="INSERT API KEY HERE")
# openai.api_type = "openai"
# openai.api_base = "https://api.openai.com/v1"
# openai.api_version = None
# openai.api_key = ""

# %%
# Read the data from the csv file
df_new = pd.read_csv('INSERT FILE NAME HERE')
# %%
## TODO removed backoff for now
from openai import RateLimitError
import backoff


@backoff.on_exception(backoff.expo, RateLimitError)
def chat_completion_with_backoff(messages):
  try:
    # response = openai.ChatCompletion.create(
    #     engine="chat",
    #     messages=messages,
    #     temperature=0.0,
    #     max_tokens=3000,
    #     top_p=0.95,
    #     frequency_penalty=0,
    #     presence_penalty=0,
    #     stop=None
    # )
    response = client.chat.completions.create(
        messages=messages,
        model="gpt-3.5-turbo-1106",
        temperature=0.0,
        max_tokens=3000,
        top_p=0.95,
        frequency_penalty=0,
        presence_penalty=0,
        response_format={"type": "json_object"},
        stop=None)
    return response
  except RateLimitError as e:
    print(f"Rate limit exceeded: {e}")
    raise  # Re-raise the RateLimitError
  except Exception as e:
    print(f"Error in OpenAI API call: {e}")
    return None


# %%
import logging

# Set up logging
logging.basicConfig(filename='error_log.txt', level=logging.ERROR)

# %%
# Process 100 rows of data at a time
chunk_size = 100
for df_100 in pd.read_csv('INSERT FILE NAME HERE', chunksize=chunk_size):
  messages = [{
      "role":
      "system",
      "content":
      "Given a list of customer's name and their respective North American Industry Classification System (NAICS) code, you are tasked with accurately assigning them to one of the following customer segments: Cheese and Other Dairy, Electronics, General Industrial, Liquids and Fluids, Logistics, Medical and Healthcare, Other Foods, Poultry, Red Meat, Retail and Consumer Products Goods, Seafood, Smoked and Processed Meats, Transportation, or Other Non-Foods. Please use your expertise in business and industry classification to make the most appropriate assignment. \n\nYour final output should be a JSON with all the objects. It should contain a list of objects with each object in the format { 'customer_name': '<Customer Name>', 'Segment': '<Assigned Segment>'}. Where '<Customer Name>' is the name of the customer and '<Assigned Segment>' is  the customer segment you have assigned based on their NAICS code.Please ensure that the output dictionary name is always 'customers' and your Assigned Segment is strictly from the list of customer segments given above, if not, please assign it as 'Other Non-Foods"
  }]
  custName = df_100['Customer Name'].tolist()
  custNameStr = ','.join(map(str, custName))
  messages.append({"role": "user", "content": custNameStr})
  print(f'messages: ', messages)
  try:
    response = chat_completion_with_backoff(messages)
    if response is None:
      logging.error(f"Failed to get a response for customer: {custName}")
      print(f"Failed to get a response for customer: {custName}. Skipping...")
      break
    content = response.choices[0].message.content
    if content is None or "The prompt was filtered due to triggering Azure OpenAI’s content filtering system" in content:
      #logging.error(f"Prompt was filtered for customer: {custName}")
      print(f"Prompt was filtered for customer: {custName}. Skipping...")
      break
    gptresponse = content
    print(gptresponse)
  except Exception as error:
    logging.error(f'Error: {error}')
    print(f'Error: {error}')
    break

  # Parse the response and write to CSV
  try:
    data_list = json.loads(gptresponse)
    customersOutputList = pyd.get(data_list, 'customers', 'error')
    if customersOutputList == 'error':
      logging.error(
          "GptResponse Format not as expected. Please fix json parsing")
      print("GptResponse Format not as expected. Please fix json parsing")
      break

    csv_file_path = 'script2Output.csv'
    if not os.path.isfile(csv_file_path):
      df = pd.DataFrame(columns=['customer_name', 'Category'])
      df.to_csv(csv_file_path, header=True, index=False, mode='w')
    pd.DataFrame(customersOutputList).to_csv(csv_file_path,
                                             header=False,
                                             index=False,
                                             mode='a')
    print(f'Data has been appended to {csv_file_path}')
  except json.JSONDecodeError as e:
    logging.error(f'JSON decode error: {e}')
    print(f'JSON decode error: {e}')
  except Exception as e:
    logging.error(f'Error: {e}')
    print(f'Error: {e}')
# %%
