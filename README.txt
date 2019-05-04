Poliviews

The goal of this project is to build a platform that will aggregate a population’s opinions on their representatives in a fair, politically unbiased fashion. In my forays into the political landscape, I have noticed some common trends:

1)	Coverage of politicians is very partisan and VERY divisive.
2)	We don’t have an insight into a politician other than what they say. We don’t have nearly the level of insight into the daily goings-on of our politicians other than what publications decide to pick up. 

I want to re-frame the questions of how we evaluate politicians’ performance based on what the do rather than what they say. What bills are they involved in writing? How are they voting on bills that they did not write? 
But most importantly, how does the public feel about what they are doing? There are very few places where public sentiment around politicians are incorporated, and normally it is based on initial perceptions around the person or party affiliation. The goal of this is to remove our feelings about a party or a politician and look at if our politicians and government as a whole are providing legislation that moves the country forward in the way that the public feels is best. This will ideally provide a better picture in electing new officials and potentially informing politicians on how their constituents would want them to vote. 

This project is being developed in 3 stages:

1)	Building and automating the web scrapes and Beam pipelines. This has been completed and is all housed in this Github repository.
2)	Initial display of the data. As a proof of concept of data visualization, I will build out a Django/Flask site in order to display the insights into the most recent congressional happenings and how data breakouts will be displayed for each politician and each bill. We can also build out a framework for how the public’s data will flow in once we start receiving responses. The design is being reviewed and will go into production soon.
3)	Incorporate input from individuals to build picture of general political perceptions. We will have statistical models built on the back-end to determine public perception of proposed and previously-voted legislation.

This project will continue to evolve as more parts are added. This outline will be updated as new sections are added.

Directory Overview
The subdirectories of this project are broken down as follows:
/web scrapers : This is the backbone of the project. This houses all Scrapy spiders that will perform daily scrapes of the necessary sites.
/runner_pipelines : These scripts receive the CSVs generated from the spiders and will re-format, process, and load the data into a Google BigQuery repository. 
/temp_pipelines : Houses the initial tests of the Dataflow pipelines. This will be updated to Beam’s testing capabilities because this used to be a streaming pipeline, which Beam is unable to test.
/dataflow_mgmt_automation : The entire project is housed on a Google Compute Engine instance. With the scripts in this file, we can run screen instances that will monitor activity in the background and activate daily spider crawls and Beam DirectRunner pipelines.
