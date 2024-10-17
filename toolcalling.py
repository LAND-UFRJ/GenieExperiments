import openai
import genieacs
import json

client = openai.OpenAI(api_key="landufrj123", base_url="http://10.246.3.122:8080/v1")

#

def get_id()->list:
    acs = genieacs.Connection("10.246.3.119", auth=True, user="admin", passwd="admin", port="7557")
    devices = acs.device_get_all_IDs()
    return devices

print(get_id())

def run_conversation():
    # Step 1: send the conversation and available functions to the model
    messages = [
        {
            "role": "user",
            "content": "What's the id from all devices? ", #prompt
        }
    ]
    tools = [
        {
            "type": "function",
            "function": {
                "name": "get_id",
                "description": "Get all IDs from devices",
                "parameters": {
                    "type": "object",
                    "properties": {
                        
                    },
                    "required": ["ID"]
                }
            }
        }
    ]

    response = client.chat.completions.create(
        model="gpt-3.5-turbo-1106",
        messages=messages,
        tools=tools,
        temperature=0,
        #tool_choice='required'
    )
    response_message = response.choices[0].message
    tool_calls = response_message.tool_calls
    # Step 2: check if the model wanted to call a function
    if tool_calls:
        # Step 3: call the function
        # Note: the JSON response may not always be valid; be sure to handle errors
        available_functions = {
            #"maxi_tool": maxi_tool,
            #"mini_tool": mini_tool,
            #"mean_tool": mean_tool,
            #"std_tool": std_tool,
        }  
        messages.append(response_message)  # extend conversation with assistant's reply
        # Step 4: send the info for each function call and function response to the model
        for tool_call in tool_calls:
            function_name = tool_call.function.name.split('\n')[0].split('.')[-1]
            function_args = json.loads(tool_call.function.name.split('\n')[1])
            function_to_call = available_functions[function_name]
            function_response = function_to_call(
                column=function_args.get("column"),
            )
            messages.append(
                {
                    "tool_call_id": tool_call.id,
                    "role": "tool",
                    "name": function_name,
                    "content": str(function_response),
                }
            )  # extend conversation with function response
        second_response = client.chat.completions.create(
            model="gpt-3.5-turbo-1106",
            messages=messages,
        )  # get a new response from the model where it can see the function response
        return second_response.choices[0].message.content


#print(run_conversation())