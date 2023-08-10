import os.path

from PIL import Image
from pytesseract import pytesseract
import numpy as np
import cv2


def getText(filepath):
      path_to_tesseract = r"C:\Program Files\Tesseract-OCR\tesseract.exe"
      img = Image.open(filepath)
      cvimg = cv2.imread(filepath)
      pytesseract.tesseract_cmd = path_to_tesseract

      data = pytesseract.image_to_data(img, output_type='dict')

      return data, cvimg





def Text_Detect(filepath):
      data, img = getText(filepath)
      text_arr = []

   #   for i in range(len(data['text'])):
   #         if data['text'][i] != '':
   #               text_arr.append(data['text'])
                  #print(data['text'])
                  #height, width, _ = cvimg.shape
                  #text = [int(data['left'][i]),int(data['top'][i]), int(data['width'][i]), int(data['height'][i])]
                  # cv2.rectangle(cvimg, (text[0], text[1]), (text[0] + text[2], text[1] + text[3]), (0, 0, 0), -1)

                  # print(data['left'][i], data['top'][i], data['width'][i], data['height'][i], data['text'][i])
      #cv2.imwrite(output_path, cvimg)

      #print(text_arr)
      return " ".join(data['text']), data, img

def Text_Process(img, output_path, processed_text, data):
      processed_data = processed_text.split(" ")
      processed_data = [item for item in processed_data if item.strip() != '']
      processed_data = [item for item in processed_data if item.strip() != ' ']

      masked_indexes = []
      sorted_indexes = []
      for index in range(0, len(processed_data)):
            if '[MASKED]' in processed_data[index]:
                  masked_indexes.append(index)

      mi_i = 0
      for i, item in enumerate(data['text']):
            if data['text'][i] != '' and data['text'][i] != ' ' and data['text'][i] != '  ':
                  if mi_i in masked_indexes:
                        sorted_indexes.append(i)

                  mi_i += 1




     # print(masked_indexes)
     # print(sorted_indexes)
     # print(processed_data)
     # print(data['text'])

      for i in range(len(data['level'])):
            if i in sorted_indexes:
                  height, width, _ = img.shape
                  text = [int(data['left'][i]),int(data['top'][i]), int(data['width'][i]), int(data['height'][i])]
                  cv2.rectangle(img, (text[0], text[1]), (text[0] + text[2], text[1] + text[3]), (0, 0, 0), -1)

      # print(data['left'][i], data['top'][i], data['width'][i], data['height'][i], data['text'][i])
      cv2.imwrite(output_path, img)

