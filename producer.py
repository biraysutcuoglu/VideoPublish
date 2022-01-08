import time
import cv2
import sys
from kafka import KafkaProducer
import sounddevice as sd
from scipy.io.wavfile import write
import sys
import soundfile as sf

topic = "test-video"

def publishVideo(video_file):
        producer = KafkaProducer(bootstrap_servers='localhost:9092')
        video = cv2.VideoCapture(video_file)
        print("Publishing video")
        while(video.isOpened()):
                success, frame = video.read()
                if not success:
                        print('bad read')
                        break
                #convert image to png
                ret, buffer = cv2.imencode('.jpg', frame)
                producer.send(topic, buffer.tobytes())

                time.sleep(0.033)
        video.release()
        print('publish complete')

def publishCamera():
        producer = KafkaProducer(bootstrap_servers='localhost:9092')
        camera = cv2.VideoCapture(0)
        try:
                while(True):
                    success, frame = camera.read()
                    ret, buffer = ret, buffer = cv2.imencode('.jpg', frame)
                    producer.send(topic, buffer.tobytes())
                    time.sleep(0.033)
        except:
                print("\nExiting.")
                sys.exit(1)

        camera.release()

def sendAudio():
        producer = KafkaProducer(bootstrap_servers='localhost:9092')
        fs = 44100  # Sample rate
        seconds = 3  # Duration of recording

        try:
                while(True):
                        myrecording = sd.rec(seconds * int(fs), samplerate=fs, channels=1)
                        print(myrecording.tobytes())
                        producer.send(topic, myrecording.tobytes())

        except:
                print("\nExiting.")
                sys.exit(1)



        '''sd.wait()  # Wait until recording is finished
        write('output.wav', fs, myrecording)  # Save as WAV file

        data, fs = sf.read('output.wav', dtype='float32')
        sd.play(data, fs)
        status = sd.wait()  # Wait until file is done playing'''



#extractSound()
publishVideo('shapes.mp4')
publishCamera()











