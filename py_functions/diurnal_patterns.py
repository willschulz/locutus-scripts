# #for diurnal probability function
# import numpy as np
# from datetime import datetime
# import random

# def generate_diurnal_probability(current_time, amplitude=0.5, phase=0, offset=0.5):
#     # Calculate the minute_of_day from the current_time
#     minute_of_day = current_time.hour * 60 + current_time.minute
    
#     # Convert minute_of_day to radians (period of 24 hours)
#     radian = 2 * np.pi * minute_of_day / (24 * 60)
    
#     # Calculate the probability using a sine function
#     probability = amplitude * np.sin(radian + phase) + offset
    
#     # Ensure probability is within [0, 1] range
#     probability = np.clip(probability, 0, 1)
    
#     return probability

# # Get the probability of action at current time
# #generate_diurnal_probability(datetime.now())

# #event_weight = .001 #this was a good event weight in a previous study
# #event_weight = .01 #this is good for testing

# def diurnal_event(current_time, event_weight):
#     return generate_diurnal_probability(current_time)*event_weight > random.random()

# #event = diurnal_event(datetime.now(), .001)
# #print(event)

# import time
# # check for a diurnal event every second, and print the result
# # while True:
# #     if diurnal_event(datetime.now(), .001):
# #         print("Event!")
# #     time.sleep(1)
# #     print("No event.")

# # now do this for exactly 1 minute
# start_time = time.time()
# while time.time() - start_time < 60:
#     if diurnal_event(datetime.now(), .01):
#         print("Event!")
#     time.sleep(1)
#     print("No event.")

import numpy as np
from datetime import datetime
import random
import threading
import time

# these defaults create a plausible diurnal shape (that then needs to be weighted according to the event to be triggered)
def generate_diurnal_probability(current_time, amplitude=.2, phase=1.2, offset=0.18, clip_floor=.01, clip_ceiling=.3):
    minute_of_day = current_time.hour * 60 + current_time.minute
    radian = 2 * np.pi * minute_of_day / (24 * 60)
    probability = amplitude * np.sin(radian + phase*3.14) + offset
    probability = np.clip(probability, clip_floor, clip_ceiling)
    return probability

def diurnal_event(current_time, event_weight):
    weighted_diurnal_probability = generate_diurnal_probability(current_time) * event_weight
    print(f"Weighted probability: {weighted_diurnal_probability}")
    #print(f"Weighted probability: {round(weighted_diurnal_probability, 3)}")
    return weighted_diurnal_probability > random.random()

def execute_with_diurnal_prob(function_to_run, args=(), kwargs={}, event_weight=0.01, duration=60):
    """
    Executes a given function with diurnal probability over a specified duration.

    :param function_to_run: The function to execute when the event triggers.
    :param args: Positional arguments to pass to the function.
    :param kwargs: Keyword arguments to pass to the function.
    :param event_weight: The weight of the event probability.
    :param duration: Duration in seconds to run the event checking loop.
    """
    start_time = time.time()
    while time.time() - start_time < duration:
        if diurnal_event(datetime.now(), event_weight):
            # Start the function in a separate thread
            threading.Thread(target=function_to_run, args=args, kwargs=kwargs).start()
            print("Event triggered, function started.")
        else:
            print("No event.")
        time.sleep(1)

# # Example function to run when the event triggers
# def my_function(arg1, arg2):
#     print(f"Function is running with arg1={arg1} and arg2={arg2}")
#     # Simulate some work
#     time.sleep(5)
#     print("Function finished")

# Usage example
#execute_with_diurnal_prob(my_function, args=(1, 2), event_weight=.1, duration=60)
