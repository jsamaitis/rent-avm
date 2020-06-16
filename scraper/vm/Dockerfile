FROM ubuntu:latest

RUN apt-get update && apt-get install -y \
	python3-pip \ 
	wget

# Variable is needed to install tzdata on Docker, which is required to install Chrome.
RUN DEBIAN_FRONTEND="noninteractive" apt-get install -y tzdata

# Install Chrome.
RUN wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
RUN apt-get install -y ./google-chrome-stable_current_amd64.deb 	
	
COPY . .

RUN pip3 install -r requirements.txt	

CMD python3 main.py