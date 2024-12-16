import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
from kafka import KafkaConsumer
import json
import matplotlib.pyplot as plt
from IPython.display import display, clear_output
import datetime
########################################################

# Setup Kafka consumer
consumer = KafkaConsumer(
    'output',
    bootstrap_servers='kafka:9092',  # Adjust to match your Kafka setup
    auto_offset_reset='latest',
    group_id='my-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Initialize lists to store data for plotting
timestamps = []
avg_temperatures = []
avg_humidities = []

# Set up the figure and axes for plotting
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 8))

def update_plots():
    # Clear the output and redraw the plots
    clear_output(wait=True)
    
    # Temperature plot
    ax1.clear()
    ax1.scatter(timestamps, avg_temperatures, color='red', label='Temperatura Promedio')
    ax1.set_title('Temperatura Promedio vs Tiempo')
    ax1.set_xlabel('Timestamp')
    ax1.set_ylabel('Temperatura (°C)')
    ax1.xaxis.set_major_locator(mdates.MinuteLocator(interval=1))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
    plt.setp(ax1.get_xticklabels(), rotation=45, ha='right')
    
    # Humidity plot
    ax2.clear()
    ax2.scatter(timestamps, avg_humidities, color='blue', label='Humedad Promedio')
    ax2.set_title('Humedad Promedio vs Tiempo')
    ax2.set_xlabel('Timestamp')
    ax2.set_ylabel('Humedad (%)')
    ax2.xaxis.set_major_locator(mdates.MinuteLocator(interval=1))
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
    plt.setp(ax2.get_xticklabels(), rotation=45, ha='right')
    
    plt.tight_layout()
    display(fig)

# Real-time update loop
for message in consumer:
    data = message.value
#     print(data)
#     break
    timestamp = pd.to_datetime(data['window']['start'])
    avg_temperature = data.get('avg_temperature')
    avg_humidity = data.get('avg_humidity')
    
    # Append new data
    timestamps.append(timestamp)
    avg_temperatures.append(avg_temperature)
    avg_humidities.append(avg_humidity)
    
    # Update plots
    update_plots()