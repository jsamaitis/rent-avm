FROM ubuntu:latest

# JOIN THIS WITH BOTTOM, ONLY FOR TESTING NOW.
RUN apt-get update

RUN apt-get install -y \
	python3-pip \ 
	wget
	
RUN DEBIAN_FRONTEND="noninteractive" apt-get -y install tzdata
RUN wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
RUN apt-get install -y ./google-chrome-stable_current_amd64.deb 	
	
COPY . .

RUN pip3 install -r requirements.txt	

CMD python3 main.py