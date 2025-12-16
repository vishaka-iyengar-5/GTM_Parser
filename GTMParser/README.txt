Hello there! Glad to see you here! 

PROJECT DESCRIPTION : This repository contains the code and data for analyzing Google Tag Manager (GTM) implementation and consent mode usage across 4,500 e-commerce websites. For detailed methodology and findings, see the accompanying dissertation.

Here's how to set up the environment, compile the code, reference the files and generate results!

1. The requirements.txt file contains all the dependencies for this project. 
	I have used docker for containerization so that its easy for people to replicate this work, regardless of device constarints.
	The docker-compose.yml sets up the docker conatiner. The Dockerfile contains all the commands and comments to install all dependencies. 

2. The Code
	
	---------
	1. simple_detector.py 
		This file contains the GTM detector, the Consent Mode detector, the third-party trackers and domains detector, google url counts, the Ghostery Tracker DB integration, and the caching mechanism. All sections have been explained in detail within the Dissertation file.  
		Each section is well commented and an attempt has been made to categorise and label it accurately within the code. 

	---------
	2. main.py 
		This is the main executable file. It pulls data from the data folder of this project. The data it pulls is categorised into the following folders:
			a. ecommerce_urls 
				- This folder contains a file called "2025-06-01 10k Unique e-commerce websites -csv" This is the seed file. As the name suggests contains the 10k websites by popularity ranking that was collected from The HTTP Archive using BigQuery Database as on 1 June 2025. 
				- This file simply contains the URLs, the website popularity ranking based on the Chrome User Experience Report and the e-commerce platform that was detected by Wappalyzer. 
				- The simple detector parses through Website number 1-1500 for set 1, 4250-5750 for set 2, and 8500-10000 for set 3. 
			b. fallback 
				- This folder contains the ghostery_trackerdb_backup_july2025.zip file. In case the Ghostery tracker DB can not be accesses online, and the caching system does not have a valid cache to refer back to, this file shall be used to identify and maintain the tracker list. 
				- The exact mechanism of the Tracker detection system has been detailed in the Dissertation file. 
			c. trackerdb 
				- This folder contains the cached tracker db file. The cache gets deleted after 1 week, and is considered valid (the code won't attempt to download the tracker db again) for 1 day. 


		The main.py file creates user agents using windows and mac OS, launches a browser using stealth mode, and itroduces random human like delays and interactions such as a page scroll to avoid detection. 

		The unique ecommerce urls can be binned into batches of customizable sizes, with a customizable number of batches to run at once. We have adopted sequential processing in batches simply because parallel processing generated too many false positives. 

		The output of each batch is written to the csv, cleared from memory - freeing up space to save the data from the next batch. The output should be a singular file with data from all the batches you had run in a single command. It is recommended that you do not run too many batches at once. I did it in numerous batches to ensure all the data was generated appropriately. 

		The results of the parser are stored in output ---> csv ---> further batched into which set they belong to. 

		To test the code we have the following commands that can be run in the terminal window :
			python main.py --test                    							# Original 4 test URLs
            python main.py --comprehensive           							# Comprehensive 13 URLs
            python main.py --batch-test              							# 300 URLs (batches 1-3)
            python main.py --full-ecommerce          							# Full 10k e-commerce analysis
            python main.py --full-ecommerce --start-batch=5 --num-batches=5  	# Batches 5-9 only


    ---------
    3. progress_manager.py
    	Contains the code to track progress during batch processing to ensure the data is not lost if errors arise. 

3. Output of the detector 
	The data that was collected using the GTM parser code is stored in output---> DataCompilation. 
	Within this folder we have the data as 3 separate sets in their respective folders, and as a combined file with all 4500 websites' data - file name = final_combined_data_4500.py in the folder FINAL_DATA


Hope this was helpful!