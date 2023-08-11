import cv2
import easyocr
import matplotlib.pyplot as plt
import numpy as np
from PIL import Image

reader = easyocr.Reader(['en'], gpu=False)
result = reader.readtext('images/Test.jpg')

# Create lists to store words, coordinates, and confidence scores
words = []
coordinates = []
confidences = []

# Iterate through each result entry
for detection in result:
    text = detection[1]  # Extract the recognized text
    bbox = detection[0]  # Extract the bounding box coordinates
    confidence = detection[2]  # Extract the confidence score

    words.append(text)
    coordinates.append(bbox)
    confidences.append(confidence)

# Print the extracted words and their coordinates
for i in range(len(words)):
    print(f"Word: {words[i]}, Coordinates: {coordinates[i]}, Confidence: {confidences[i]}")
