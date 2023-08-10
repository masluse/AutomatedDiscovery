import cv2
import easyocr
import matplotlib.pyplot as plt


img = cv2.imread('images/Test.jpg')

reader = easyocr.Reader(['en'], gpu=False)

text = reader.readtext(img)

print(text)