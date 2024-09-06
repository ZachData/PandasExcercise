Requirements:
	Ubuntu 18.04
	pandas 1.1.5

Although Windows users are strongly encouraged to use Docker within WSL,
	they may find full functinality using Pandas 1.2.2 and without using any form of Docker.

commands to build dockerfile:
	Please place the files into a folder. The name 'src' is recommended. Files are:
		main.py
		utils.py
		dataset.py
		read_data.py
		Dockerfile
		ReadMe.txt
		Data.csv

	Once in the directory containing the files, run:
		sudo docker build -f Dockerfile --tag challenge_img . 
			This builds the docker image (challenge_img) using Ubuntu 18.04 and pandas 1.1.5

	To run the code:
		sudo docker run -w /src/ challenge_img python3 main.py
			This builds a container from the image, and then runs the main file in python.
			Feel free to add arguments, 
				with one caveat:
			To try manual entry, one must enter an interactive terminal. use commands:
				sudo docker run -it -w /src/ challenge_img
				python3 main.py --data_format manual_entry